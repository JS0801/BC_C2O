/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 *
 * C2O Day Rate Hours Aggregation
 *
 * Nightly Map/Reduce. For each enabled day-rate project in
 * customrecord_c2o_dayrate_config, groups unprocessed working TimeBills
 * (ST / OT / DT) by employee + work date + project, sums hours, and
 * replaces them with 1–2 aggregated TimeBills:
 *   hours up to threshold   -> ST
 *   hours beyond threshold  -> OT
 *
 * Originals stay intact (for payroll/costing) but are flagged non-billable
 * with an audit link to the new record.
 */
define(['N/search', 'N/record', 'N/log'], function (search, record, log) {

    // ─── Field / record IDs ──────────────────────────────────────
    var CONFIG_RECORD  = 'customrecord_c2o_dayrate_config';
    var CFG_PROJECT    = 'custrecord_bc_dayrate_project';
    var CFG_ENABLED    = 'custrecord_bc_dayrate_enabled';
    var CFG_THRESHOLD  = 'custrecord_bc_dayrate_threshold';

    var TB_PROJECT     = 'cseg_bc_project';                       // project on TimeBill
    var TB_TIME_TYPE   = 'custcol_bc_time_type';                  // list: 1=OT, 2=DT, 3=ST...
    var TB_BILLED_TRAN = 'custcol_bc_timebill_tm_billing_tran';   // if filled => already processed
    var TB_VIA_SFTP    = 'custcol_bc_created_via_sftp';           // only aggregate SFTP-created TBs
    var TB_SOURCE_IDS  = 'custcol_c2o_dr_source_ids';             // aggregated -> list of source IDs
    var TB_AGG_BY      = 'custcol_c2o_dr_aggregated_by';          // source   -> replacement ID(s)

    // custcol_bc_time_type internal IDs (from C2O list)
    var TT_OT = '1';
    var TT_DT = '2';
    var TT_ST = '3';

    // ─── Stage 1: getInputData ───────────────────────────────────
    function getInputData() {

      try {
        log.audit('getInputData', 'Start — loading enabled day-rate projects');

        var projects = loadEnabledProjects();
        log.audit('getInputData', 'Enabled projects: ' + projects.length);

        if (projects.length === 0) {
            log.audit('getInputData', 'Nothing to do — no enabled projects.');
            return [];
        }

        var projectIds = projects.map(function (p) { return p.projectId; });

        return search.create({
            type: 'timebill',
            filters: [
                ['datecreated',  'on',      'today'],                'AND',
                ['line.cseg_bc_project','anyof',   projectIds],      'AND',
                [TB_TIME_TYPE,   'anyof',   [TT_ST, TT_OT, TT_DT]],  'AND',
                [TB_BILLED_TRAN, 'isempty', ''],                     'AND',
                [TB_VIA_SFTP,    'is',      'T'],                    'AND',
                [TB_SOURCE_IDS,  'isempty', ''],                     'AND',  // skip already-aggregated
                [TB_AGG_BY,      'isempty', '']                              // skip already-consumed
            ],
            columns: ['employee', 'date', 'line.cseg_bc_project', TB_TIME_TYPE, 'hours']
        });
        
      } catch (error) {
        log.error('Get-Error', error)
      }
    }

    // ─── Stage 2: map — group by employee|date|project ───────────
    function map(context) {
        try {
          log.debug('context', context)
            var r = JSON.parse(context.value);
            log.debug('r', r)

            var v = {
                tbId:      r.id,
                employee:  r.values.employee.value,
                tranDate:  r.values.date,
                projectId: r.values['line.cseg_bc_project'].value,
                timeType:  r.values[TB_TIME_TYPE].value,
                hours:     parseFloat(r.values.hours) || 0
            };

            var key = v.employee + '|' + v.tranDate + '|' + v.projectId;

            log.debug('map', 'TB ' + v.tbId + ' -> ' + key + ' (' + v.hours + 'h, type ' + v.timeType + ')');
            context.write({ key: key, value: JSON.stringify(v) });

        } catch (e) {
            log.error('map error, value=' + context.value, e);
        }
    }

    // ─── Stage 3: reduce — aggregate each group ──────────────────
    function reduce(context) {
        log.debug('Reduce context', context)
        var key = context.key;
        try {
            var entries = context.values.map(function (s) { return JSON.parse(s); });

            log.audit('reduce', 'key=' + key + ' entries=' + entries.length);

            // Only aggregate when there is more than one working TimeBill.
            if (entries.length < 2) {
                log.debug('reduce', 'Skip — single TimeBill for ' + key);
                return;
            }

            // Sum hours and collect source IDs
            var totalHours = 0;
            var sourceIds = [];
            entries.forEach(function (e) {
                totalHours += e.hours;
                sourceIds.push(e.tbId);
            });

            var projectId = entries[0].projectId;
            var threshold = getThreshold(projectId);
            if (threshold === null) {
                log.error('reduce', 'No threshold for project ' + projectId + ' — skipping ' + key);
                return;
            }

            var stHours = Math.min(totalHours, threshold);
            var otHours = Math.max(0, totalHours - threshold);

            log.audit('reduce',
                'key=' + key +
                ' total=' + totalHours +
                ' threshold=' + threshold +
                ' -> ST=' + stHours + ' OT=' + otHours +
                ' sources=[' + sourceIds.join(',') + ']'
            );

            // Use first source as field template (same employee+date+project => same item/class/etc.)
            var template = record.load({ type: record.Type.TIME_BILL, id: sourceIds[0] });

            var newIds = [];
            if (stHours > 0) newIds.push(createAggregated(template, stHours, TT_ST, sourceIds));
            if (otHours > 0) newIds.push(createAggregated(template, otHours, TT_OT, sourceIds));

            // Back-link + mark originals non-billable
            var aggRef = newIds.join(',');
            sourceIds.forEach(function (id) {
                try {
                    var vals = { isbillable: false };
                    vals[TB_AGG_BY] = aggRef;
                    record.submitFields({
                        type: record.Type.TIME_BILL,
                        id: id,
                        values: vals,
                        options: { enableSourcing: false, ignoreMandatoryFields: true }
                    });
                    log.debug('reduce', 'Source TB ' + id + ' -> non-billable, aggregated_by=' + aggRef);
                } catch (e) {
                    log.error('reduce: update source TB ' + id + ' failed', e);
                }
            });

        } catch (e) {
            log.error('reduce error, key=' + key, e);
        }
    }

    // ─── Stage 4: summarize ──────────────────────────────────────
    function summarize(summary) {
        log.audit('summarize', 'Map/Reduce complete.');
        log.audit('summarize', 'Map keys:    ' + summary.mapSummary.keys);
        log.audit('summarize', 'Reduce keys: ' + summary.reduceSummary.keys);
        log.audit('summarize', 'Usage units: ' + summary.usage);

        if (summary.inputSummary.error) {
            log.error('summarize: input error', summary.inputSummary.error);
        }
        summary.mapSummary.errors.iterator().each(function (k, err) {
            log.error('summarize: map error key=' + k, err);
            return true;
        });
        summary.reduceSummary.errors.iterator().each(function (k, err) {
            log.error('summarize: reduce error key=' + k, err);
            return true;
        });
    }

    // ─── Helpers ─────────────────────────────────────────────────

    function loadEnabledProjects() {
        var out = [];
        search.create({
            type: CONFIG_RECORD,
            filters: [[CFG_ENABLED, 'is', 'T']],
            columns: [CFG_PROJECT, CFG_THRESHOLD]
        }).run().each(function (r) {
            var pid = r.getValue(CFG_PROJECT);
            if (pid) {
                out.push({
                    projectId: pid,
                    threshold: parseFloat(r.getValue(CFG_THRESHOLD)) || 0
                });
            }
            return true;
        });
        return out;
    }

    // Lazy cache so each reduce execution context loads config at most once.
    var _thresholdCache = null;
    function getThreshold(projectId) {
        if (!_thresholdCache) {
            _thresholdCache = {};
            loadEnabledProjects().forEach(function (p) {
                _thresholdCache[p.projectId] = p.threshold;
            });
            log.debug('getThreshold', 'Cache loaded: ' + JSON.stringify(_thresholdCache));
        }
        return _thresholdCache.hasOwnProperty(projectId) ? _thresholdCache[projectId] : null;
    }

    function createAggregated(template, hours, timeType, sourceIds) {
        var rec = record.create({ type: record.Type.TIME_BILL });

        // Copy billing-relevant fields from the template source TB.
        // Add/remove fields here once the billing tool's required set is confirmed in sandbox (Risk #1).
        copyIfPresent(template, rec, [
            'employee', 'trandate', 'customer', 'item', 'memo',
            'class', 'department', 'location', 'subsidiary',
            'custcol_bc_tm_billing_shift', 'cseg_bc_project', 'cseg_bc_cost_code'
        ]);

        rec.setValue({ fieldId: 'hours',       value: hours });
        rec.setValue({ fieldId: TB_TIME_TYPE,  value: timeType });
        rec.setValue({ fieldId: 'isbillable',  value: true });
        rec.setValue({ fieldId: TB_SOURCE_IDS, value: sourceIds.join(',') });

        var id = rec.save({ ignoreMandatoryFields: true });
        log.audit('createAggregated',
            'Created TB ' + id + ' (' + hours + 'h, type ' + timeType +
            ', sources=[' + sourceIds.join(',') + '])'
        );
        return id;
    }

    function copyIfPresent(src, dst, fieldIds) {
        fieldIds.forEach(function (f) {
            try {
                var v = src.getValue({ fieldId: f });
                if (v !== null && v !== '' && v !== undefined) {
                    dst.setValue({ fieldId: f, value: v });
                }
            } catch (ignore) { /* field not present on record — skip silently */ }
        });
    }

    return {
        getInputData: getInputData,
        map:          map,
        reduce:       reduce,
        summarize:    summarize
    };
});
