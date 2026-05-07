/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 * @NModuleScope SameAccount
 *
 * C2O Day Rate Aggregation — Reconciliation Dashboard
 *
 * Single-page Suitelet for finance & audit users. Mirrors the field/record
 * IDs used by BC_Dayrate_Engine.js so the two stay in sync.
 *
 *   Section 1 — Filters         (date range, config, project, employee)
 *   Section 2 — Configs sublist (every customrecord_c2o_dayrate_config)
 *   Section 3 — Stats summary   (group counts, hour totals, status counts)
 *   Section 4 — Reconciliation  (per employee+date+project group:
 *                                aggregated TBs paired with source TBs,
 *                                with a hours-match indicator)
 *
 * Read-only. No record writes. Safe for non-admin roles.
 *
 * Deployment:
 *   - Script record: SuiteScript 2.1, type Suitelet
 *   - Deployment:    Available without login = N (admin/finance only)
 *   - Audience:      whichever roles need read-only audit access
 */
define([
    'N/ui/serverWidget',
    'N/search',
    'N/url',
    'N/format',
    'N/log'
], function (ui, search, url, format, log) {

    // ─── Field / record IDs (must match BC_Dayrate_Engine.js) ────────
    var CONFIG_RECORD = 'customrecord_c2o_dayrate_config';
    var CFG_PROJECT   = 'custrecord_bc_dayrate_project';
    var CFG_ENABLED   = 'custrecord_bc_dayrate_enabled';
    var CFG_THRESHOLD = 'custrecord_bc_dayrate_threshold';

    var TB_PROJECT_LINE = 'line.cseg_bc_project';
    var TB_TIME_TYPE    = 'custcol_bc_time_type';
    var TB_SOURCE_IDS   = 'custcol_c2o_dr_source_ids';
    var TB_AGG_BY       = 'custcol_c2o_dr_aggregated_by';

    // custcol_bc_time_type internal IDs (from C2O list)
    var TT_LABELS = { '1': 'OT', '2': 'DT', '3': 'ST' };

    // Search safety + UI cap
    var MAX_ROWS         = 4000;
    var MAX_GROUPS_SHOWN = 250;

    // Project column for line-level join (used in both filters and getValue)
    var PROJECT_COL = { name: 'cseg_bc_project', join: 'line' };

    // ─── Entry point ─────────────────────────────────────────────────
    function onRequest(context) {
        try {
            var filters = parseFilters(context.request);

            var form = ui.createForm({ title: 'C2O Day Rate — Reconciliation Dashboard' });

            buildFilterSection(form, filters);
            buildConfigSublist(form);

            var data = loadReconciliationData(filters);
            buildStatsBlock(form, data);
            buildReconciliationGrid(form, data);

            form.addSubmitButton({ label: 'Apply Filters' });
            context.response.writePage(form);

        } catch (e) {
            log.error('Suitelet onRequest', e);
            context.response.write({
                output: '<h2>Dashboard error</h2><pre>' + escapeHtml(e.message + '\n' + (e.stack || '')) + '</pre>'
            });
        }
    }

    // ─── Filter parsing ──────────────────────────────────────────────
    function parseFilters(req) {
        var p = req.parameters || {};
        var today = new Date();
        var defaultFrom = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);

        return {
            dateFrom:   p.custpage_date_from || formatDate(defaultFrom),
            dateTo:     p.custpage_date_to   || formatDate(today),
            configId:   p.custpage_config    || '',
            projectId:  p.custpage_project   || '',
            employeeId: p.custpage_employee  || ''
        };
    }

    function formatDate(d) {
        return format.format({ value: d, type: format.Type.DATE });
    }

    // ─── UI: filter section ──────────────────────────────────────────
    function buildFilterSection(form, filters) {
        form.addFieldGroup({ id: 'fg_filters', label: 'Filters' });

        var fDateFrom = form.addField({
            id: 'custpage_date_from', type: ui.FieldType.DATE,
            label: 'Date From', container: 'fg_filters'
        });
        fDateFrom.defaultValue = filters.dateFrom;

        var fDateTo = form.addField({
            id: 'custpage_date_to', type: ui.FieldType.DATE,
            label: 'Date To', container: 'fg_filters'
        });
        fDateTo.defaultValue = filters.dateTo;

        var fConfig = form.addField({
            id: 'custpage_config', type: ui.FieldType.SELECT,
            label: 'Day-Rate Config', source: CONFIG_RECORD, container: 'fg_filters'
        });
        fConfig.defaultValue = filters.configId;

        var fProject = form.addField({
            id: 'custpage_project', type: ui.FieldType.SELECT,
            label: 'Project', source: 'project', container: 'fg_filters'
        });
        fProject.defaultValue = filters.projectId;

        var fEmployee = form.addField({
            id: 'custpage_employee', type: ui.FieldType.SELECT,
            label: 'Employee', source: 'employee', container: 'fg_filters'
        });
        fEmployee.defaultValue = filters.employeeId;
    }

    // ─── UI: configs sublist ─────────────────────────────────────────
    function buildConfigSublist(form) {
        var sub = form.addSublist({
            id: 'custpage_configs',
            label: 'Day-Rate Configurations',
            type: ui.SublistType.LIST
        });
        sub.addField({ id: 'col_id',        label: 'ID',        type: ui.FieldType.TEXT });
        sub.addField({ id: 'col_project',   label: 'Project',   type: ui.FieldType.TEXT });
        sub.addField({ id: 'col_threshold', label: 'Threshold', type: ui.FieldType.FLOAT });
        sub.addField({ id: 'col_enabled',   label: 'Enabled',   type: ui.FieldType.TEXT });
        sub.addField({ id: 'col_link',      label: 'Open',      type: ui.FieldType.URL });

        try {
            var idx = 0;
            search.create({
                type: CONFIG_RECORD,
                filters: [],
                columns: [CFG_PROJECT, CFG_THRESHOLD, CFG_ENABLED]
            }).run().each(function (r) {
                var enabledRaw = r.getValue(CFG_ENABLED);
                var enabled = (enabledRaw === true || enabledRaw === 'T' || enabledRaw === 't');

                sub.setSublistValue({ id: 'col_id',        line: idx, value: r.id });
                sub.setSublistValue({ id: 'col_project',   line: idx, value: r.getText(CFG_PROJECT) || '—' });
                sub.setSublistValue({ id: 'col_threshold', line: idx, value: r.getValue(CFG_THRESHOLD) || '0' });
                sub.setSublistValue({ id: 'col_enabled',   line: idx, value: enabled ? 'Yes' : 'No' });
                sub.setSublistValue({
                    id: 'col_link', line: idx,
                    value: url.resolveRecord({ recordType: CONFIG_RECORD, recordId: r.id })
                });
                idx++;
                return true;
            });
        } catch (e) {
            log.error('buildConfigSublist', e);
        }
    }

    // ─── Data loading ────────────────────────────────────────────────
    function loadReconciliationData(filters) {
        var sFilters = [
            ['date', 'within', filters.dateFrom, filters.dateTo],
            'AND',
            [
                [TB_SOURCE_IDS, 'isnotempty', ''],
                'OR',
                [TB_AGG_BY,     'isnotempty', '']
            ]
        ];

        // Config filter resolves to a project ID (every config row has one)
        var projectId = filters.projectId;
        if (filters.configId) {
            try {
                var lookup = search.lookupFields({
                    type: CONFIG_RECORD,
                    id: filters.configId,
                    columns: [CFG_PROJECT]
                });
                var arr = lookup[CFG_PROJECT];
                if (arr && arr.length) projectId = arr[0].value;
            } catch (e) {
                log.error('config lookup', e);
            }
        }
        if (projectId) {
            sFilters.push('AND', [TB_PROJECT_LINE, 'anyof', projectId]);
        }
        if (filters.employeeId) {
            sFilters.push('AND', ['employee', 'anyof', filters.employeeId]);
        }

        var rows = [];
        try {
            search.create({
                type: 'timebill',
                filters: sFilters,
                columns: [
                    'internalid', 'employee', 'date',
                    PROJECT_COL,
                    TB_TIME_TYPE, 'hours', 'isbillable',
                    TB_SOURCE_IDS, TB_AGG_BY, 'memo'
                ]
            }).run().each(function (r) {
                rows.push({
                    id:           r.id,
                    employee:     r.getText({ name: 'employee' }) || '',
                    employeeId:   r.getValue({ name: 'employee' }) || '',
                    tranDate:     r.getValue({ name: 'date' }),
                    project:      r.getText(PROJECT_COL)  || '',
                    projectId:    r.getValue(PROJECT_COL) || '',
                    timeType:     r.getValue(TB_TIME_TYPE),
                    timeTypeText: TT_LABELS[r.getValue(TB_TIME_TYPE)] || (r.getText(TB_TIME_TYPE) || '?'),
                    hours:        parseFloat(r.getValue('hours')) || 0,
                    billable:     (r.getValue('isbillable') === true || r.getValue('isbillable') === 'T'),
                    sourceIds:    r.getValue(TB_SOURCE_IDS),
                    aggregatedBy: r.getValue(TB_AGG_BY),
                    memo:         r.getValue('memo')
                });
                return rows.length < MAX_ROWS;
            });
        } catch (e) {
            log.error('loadReconciliationData search', e);
            throw e;
        }

        // Group by employee | date | project (same key as engine script)
        var groupMap = {};
        rows.forEach(function (row) {
            var key = (row.employeeId || '_') + '|' + row.tranDate + '|' + (row.projectId || '_');
            if (!groupMap[key]) {
                groupMap[key] = {
                    key: key,
                    employee: row.employee, tranDate: row.tranDate, project: row.project,
                    aggregates: [], sources: []
                };
            }
            if (row.sourceIds) {
                groupMap[key].aggregates.push(row);
            } else if (row.aggregatedBy) {
                groupMap[key].sources.push(row);
            }
        });

        var groups = [];
        Object.keys(groupMap).forEach(function (k) { groups.push(groupMap[k]); });
        groups.sort(function (a, b) {
            if (a.tranDate !== b.tranDate) return a.tranDate < b.tranDate ? 1 : -1;
            return (a.employee || '').localeCompare(b.employee || '');
        });

        // Per-group totals + status
        var totals = { groups: groups.length, aggCount: 0, srcCount: 0, aggHours: 0, srcHours: 0,
                       match: 0, mismatch: 0, pending: 0, orphan: 0 };
        groups.forEach(function (g) {
            g.aggHours = g.aggregates.reduce(function (s, r) { return s + r.hours; }, 0);
            g.srcHours = g.sources.reduce(function (s, r) { return s + r.hours; }, 0);
            g.status   = computeStatus(g);
            totals.aggCount += g.aggregates.length;
            totals.srcCount += g.sources.length;
            totals.aggHours += g.aggHours;
            totals.srcHours += g.srcHours;
            totals[g.status]++;
        });

        return { groups: groups, totals: totals, rowCount: rows.length, capped: rows.length >= MAX_ROWS };
    }

    function computeStatus(g) {
        if (g.aggregates.length === 0 && g.sources.length > 0) return 'pending';
        if (g.sources.length === 0    && g.aggregates.length > 0) return 'orphan';
        if (Math.abs(g.aggHours - g.srcHours) > 0.01) return 'mismatch';
        return 'match';
    }

    // ─── UI: stats summary ───────────────────────────────────────────
    function buildStatsBlock(form, data) {
        var t = data.totals;
        var html = ''
            + '<div style="display:flex;flex-wrap:wrap;gap:10px;padding:10px 0;font-family:Helvetica,Arial,sans-serif;">'
            +   stat('Groups',         t.groups,           '#0066cc')
            +   stat('Aggregated TBs', t.aggCount,         '#16a34a')
            +   stat('Source TBs',     t.srcCount,         '#6b7280')
            +   stat('Agg Hours',      round(t.aggHours),  '#16a34a')
            +   stat('Source Hours',   round(t.srcHours),  '#6b7280')
            +   stat('✓ Match',        t.match,            '#16a34a')
            +   stat('⚠ Mismatch',     t.mismatch,         t.mismatch ? '#dc2626' : '#6b7280')
            +   stat('⏳ Pending',     t.pending,          t.pending  ? '#d97706' : '#6b7280')
            +   stat('⚠ Orphan',       t.orphan,           t.orphan   ? '#dc2626' : '#6b7280')
            + '</div>';

        if (data.capped) {
            html += '<div style="padding:8px 12px;background:#fef3c7;border:1px solid #f59e0b;border-radius:4px;margin-bottom:8px;font-family:Helvetica,Arial,sans-serif;font-size:13px;">'
                  + '⚠ Result set capped at ' + MAX_ROWS + ' TimeBills. Narrow your date range or filters for full results.'
                  + '</div>';
        }

        var f = form.addField({ id: 'custpage_stats', type: ui.FieldType.INLINEHTML, label: 'Summary' });
        f.defaultValue = html;
    }

    function stat(label, value, color) {
        return ''
            + '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:6px;padding:10px 14px;min-width:110px;">'
            +   '<div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px;">' + escapeHtml(label) + '</div>'
            +   '<div style="font-size:22px;font-weight:600;color:' + color + ';margin-top:2px;">' + escapeHtml(String(value)) + '</div>'
            + '</div>';
    }

    // ─── UI: reconciliation grid ─────────────────────────────────────
    function buildReconciliationGrid(form, data) {
        var html = ''
            + '<style>'
            + '.recon-wrap { font-family:Helvetica,Arial,sans-serif; font-size:13px; }'
            + '.recon-grp  { border:1px solid #d1d5db; border-radius:6px; margin-bottom:14px; overflow:hidden; }'
            + '.recon-hdr  { background:#f3f4f6; padding:10px 14px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; border-bottom:1px solid #d1d5db; }'
            + '.recon-hdr-left  { font-weight:600; color:#111827; }'
            + '.recon-hdr-right { font-size:12px; color:#374151; }'
            + '.recon-tbl       { width:100%; border-collapse:collapse; }'
            + '.recon-tbl th    { background:#f9fafb; padding:6px 10px; text-align:left; font-size:11px; color:#6b7280; text-transform:uppercase; border-bottom:1px solid #e5e7eb; }'
            + '.recon-tbl td    { padding:6px 10px; border-bottom:1px solid #f3f4f6; }'
            + '.row-agg        { background:#f0fdf4; }'
            + '.row-src        { background:#fff; color:#6b7280; }'
            + '.row-src .strike{ text-decoration:line-through; }'
            + '.badge          { display:inline-block; padding:2px 8px; border-radius:10px; font-size:11px; font-weight:600; color:#fff; }'
            + '.b-agg { background:#16a34a; }  .b-src { background:#9ca3af; }'
            + '.b-st  { background:#3b82f6; }  .b-ot  { background:#f59e0b; }  .b-dt  { background:#ef4444; }'
            + '.s-match    { color:#16a34a; }  .s-mismatch { color:#dc2626; font-weight:600; }'
            + '.s-pending  { color:#d97706; }  .s-orphan   { color:#dc2626; font-weight:600; }'
            + '.empty      { padding:30px; text-align:center; color:#6b7280; font-size:14px; }'
            + '.cap-note   { padding:10px 14px; background:#f9fafb; border-top:1px solid #e5e7eb; font-size:12px; color:#6b7280; }'
            + '</style>'
            + '<div class="recon-wrap">';

        if (data.groups.length === 0) {
            html += '<div class="empty">No aggregation activity in the selected range.</div>';
        } else {
            var shown = data.groups.slice(0, MAX_GROUPS_SHOWN);
            shown.forEach(function (g) { html += renderGroup(g); });
            if (data.groups.length > MAX_GROUPS_SHOWN) {
                html += '<div class="cap-note">Showing first ' + MAX_GROUPS_SHOWN
                      + ' of ' + data.groups.length + ' groups. Narrow the date range to see more.</div>';
            }
        }
        html += '</div>';

        var f = form.addField({ id: 'custpage_recon', type: ui.FieldType.INLINEHTML, label: 'Reconciliation' });
        f.defaultValue = html;
    }

    function renderGroup(g) {
        var statusLabels = {
            match:    '✓ Hours match',
            mismatch: '⚠ Hours mismatch',
            pending:  '⏳ Pending aggregation (no aggregate yet)',
            orphan:   '⚠ Orphaned aggregate (no source records found)'
        };

        var s = ''
            + '<div class="recon-grp">'
            +   '<div class="recon-hdr">'
            +     '<div class="recon-hdr-left">' + escapeHtml(g.employee || '(unknown)')
            +       ' &nbsp;•&nbsp; ' + escapeHtml(g.tranDate || '')
            +       ' &nbsp;•&nbsp; ' + escapeHtml(g.project || '(no project)') + '</div>'
            +     '<div class="recon-hdr-right">'
            +       'Sources: <strong>' + round(g.srcHours) + 'h</strong> &nbsp;·&nbsp; '
            +       'Aggregated: <strong>' + round(g.aggHours) + 'h</strong> &nbsp;·&nbsp; '
            +       '<span class="s-' + g.status + '">' + statusLabels[g.status] + '</span>'
            +     '</div>'
            +   '</div>'
            +   '<table class="recon-tbl">'
            +     '<thead><tr>'
            +       '<th>Type</th><th>TB ID</th><th>Time Type</th><th>Hours</th>'
            +       '<th>Billable</th><th>Linked Records</th><th>Memo</th>'
            +     '</tr></thead><tbody>';

        // Aggregates first (highlighted), then sources beneath
        g.aggregates.forEach(function (a) { s += renderRow(a, 'agg'); });
        g.sources.forEach(function (sr) { s += renderRow(sr, 'src'); });

        s += '</tbody></table></div>';
        return s;
    }

    function renderRow(r, kind) {
        var typeBadge = kind === 'agg'
            ? '<span class="badge b-agg">AGG</span>'
            : '<span class="badge b-src">SRC</span>';
        var ttClass = ({ '1': 'b-ot', '2': 'b-dt', '3': 'b-st' })[r.timeType] || 'b-src';
        var ttBadge = '<span class="badge ' + ttClass + '">' + escapeHtml(r.timeTypeText) + '</span>';
        var linkText = kind === 'agg'
            ? 'Sources: '     + escapeHtml(r.sourceIds || '—')
            : 'Replaced by: ' + escapeHtml(r.aggregatedBy || '—');
        var billableTxt = r.billable ? '✓' : '—';
        var strikeCls = (kind === 'src' && !r.billable) ? ' strike' : '';
        var tbUrl = url.resolveRecord({ recordType: 'timebill', recordId: r.id });
        var tbLink = '<a href="' + tbUrl + '" target="_blank" rel="noopener">' + escapeHtml(String(r.id)) + '</a>';

        return ''
            + '<tr class="row-' + kind + '">'
            +   '<td>' + typeBadge + '</td>'
            +   '<td class="' + strikeCls + '">' + tbLink + '</td>'
            +   '<td>' + ttBadge + '</td>'
            +   '<td class="' + strikeCls + '">' + round(r.hours) + '</td>'
            +   '<td>' + billableTxt + '</td>'
            +   '<td>' + linkText + '</td>'
            +   '<td>' + escapeHtml(r.memo || '') + '</td>'
            + '</tr>';
    }

    // ─── Helpers ─────────────────────────────────────────────────────
    function round(n) {
        return (Math.round((parseFloat(n) || 0) * 100) / 100).toString();
    }

    function escapeHtml(s) {
        if (s === null || s === undefined) return '';
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    return { onRequest: onRequest };
});
