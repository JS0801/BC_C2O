/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/record', 'N/file', 'N/search', 'N/log', 'N/task'],
function(record, file, search, log, task) {

    function getInputData() {
        return search.create({
            type: 'customrecord_bc_df_ts_raw_file',
           filters: [['custrecord_bc_df_ts_status', 'anyof', '1']], // Pending
            columns: ['internalid', 'custrecord_bc_df_ts_file', 'name']
        });
    }

    function map(context) {
        var result = JSON.parse(context.value);
        var parentId = result.id;
        var fileId = result.values.custrecord_bc_df_ts_file.value;

        log.debug('map()', 'Splitting File ID: ' + fileId + ' (Parent ID: ' + parentId + ')');

        try {
            var timesheetFile = file.load({ id: fileId });
            var contents = timesheetFile.getContents().replace(/^\uFEFF/, '');
            var lines = contents.split(/\r?\n/);
            var processed = 0;

            // Write total row count for parent tracking
           /* context.write('TOTAL_' + parentId, JSON.stringify({
                parentId: parentId,
                totalRows: 0,
              parentName: result.values.name
            }));*/

            for (var i = 1; i < lines.length; i++) { // skip header
                var line = lines[i];
                if (!line || /^\s*$/.test(line)) continue; // skip truly blank rows

                processed++;
                // Unique key per row to force 1:1 reduce call
                context.write(parentId + '_' + i, JSON.stringify({
                    parentId: parentId,
                    rowNum: i,
                    line: line,
                    parentName: result.values.name
                }));
            }

            // Optionally emit the actual count (will be merged in summarize)
            context.write('TOTAL_' + parentId, JSON.stringify({
                parentId: parentId,
                totalRows: processed,
                parentName: result.values.name
            }));

        } catch (e) {
            log.debug('map() ERROR', 'Failed to process File ID: ' + fileId + ' - ' + e.message);
        }
    }

    function reduce(context) {
        if (context.key.indexOf('TOTAL_') === 0) {
            var data = JSON.parse(context.values[0]);
            context.write(data.parentId, JSON.stringify({
                type: 'total',
                totalRows: data.totalRows,
                parentName: data.parentName || 'Unknown'
            }));
            return;
        }

        var data = JSON.parse(context.values[0]);
        var parentId = data.parentId;
        var rowNum = data.rowNum;
        var line = data.line;

        log.debug('reduce()', 'Processing Row ' + rowNum + ' for Parent ID: ' + parentId);

        try {
            var cols = parseCsvLine(line);
            cols = normalizeColumns(cols, 15, 7);

            log.debug('cols[0..4]', JSON.stringify(cols.slice(0,5)));

            if (cols.length !== 15) {
                throw new Error('Column count mismatch after normalization. Got ' + cols.length + ', expected 15.');
            }

            var rowRec = record.create({ type: 'customrecord_bc_df_ts_row_data' });
            rowRec.setValue('custrecord_bc_df_ts_row_parent', parentId);
            rowRec.setValue('custrecord_bc_df_ts_row_number', rowNum);
            rowRec.setValue('custrecord_bc_df_ts_row_bc_project', cols[0] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_labor_bill_code', cols[1] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_service_item', cols[2] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_emp_id', cols[3] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_emp_name', cols[4] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_date', cols[5] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_shift_type', cols[6] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_memo', cols[7] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_duration', cols[8] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_start_time', cols[9] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_end_time', cols[10] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_department', cols[11] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_dept_class', cols[12] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_billable', cols[13] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_pay_category', cols[14] || '');
            rowRec.setValue('custrecord_bc_df_ts_row_status', 1);
            rowRec.save();

            log.debug('Row ' + rowNum, 'SUCCESS: Row Data Record created');

        } catch (e) {
            // Write errors for summarize()
            context.write(parentId, JSON.stringify({
                type: 'error',
                rowNum: rowNum,
                error: e.message,
                parentName: data.parentName || 'Unknown'
            }));
            log.debug('Row ' + rowNum, 'FAILED: ' + e.message);
        }
    }

    function summarize(summary) {
        var parentData = {};

        summary.output.iterator().each(function(parentId, value) {
            var data = JSON.parse(value);

            if (!parentData[parentId]) {
                parentData[parentId] = {
                    totalRows: 0,
                    errors: [],
                  parentName: data.parentName || 'Unknown'
                };
            }

            if (data.type === 'total') {
                parentData[parentId].totalRows = data.totalRows;
            }

            if (data.type === 'error') {
                parentData[parentId].errors.push('Row ' + data.rowNum + ': ' + data.error);
            }

            return true;
        });

        for (var parentId in parentData) {
            var totalRows = parentData[parentId].totalRows;
            var errors = parentData[parentId].errors;
            var errorFileId = null;

            if (errors.length > 0) {

               var rawParentName = parentData[parentId].parentName;
            var safeParentName = rawParentName.replace(/\.csv$/i, '') // remove .csv
                                              .replace(/[^a-zA-Z0-9_-]/g, '_'); // sanitize

              var errorFileName = 'TimeEntry_Errors_' + safeParentName + '.txt';
              
                var errorFileContent = 'Dayforce Error Log\n----------------------\n' + errors.join('\n');
                var errorFile = file.create({
                    name: errorFileName,
                    fileType: file.Type.PLAINTEXT,
                    contents: errorFileContent,
                    folder: 1663 // <-- Hardcoded Folder ID
                });
                errorFileId = errorFile.save();
                log.debug('summarize()', 'Error file created for Parent ID ' + parentId + ': File ID ' + errorFileId);
            }

            try {
                record.submitFields({
                    type: 'customrecord_bc_df_ts_raw_file',
                    id: parentId,
                    values: {
                        custrecord_bc_df_ts_status: errors.length > 0 ? 3 : 2, // 3 = Error, 2 = Done
                        custrecord_bc_df_ts_total_rows: totalRows,
                        custrecord_bc_df_ts_failed_rows: errors.length,
                        custrecord_bc_df_ts_logs_file: errorFileId
                    }
                });
                log.debug('summarize()', 'Parent ID ' + parentId + ' updated. Total rows: ' + totalRows + ', Errors: ' + errors.length);
            } catch (e) {
                log.debug('summarize() ERROR', 'Parent ID: ' + parentId + ' - ' + e.message);
            }
        }

        log.debug('summarize()', 'Stage 2 processing complete');

      try {
        var mrTask = task.create({
          taskType: task.TaskType.MAP_REDUCE,
          scriptId: 'customscript_bc_df_mr_create_time_entry',
          deploymentId: 'customdeploy_bc_df_mr_create_time_entry'
        });
        
        var taskId = mrTask.submit();
        log.debug('Triggered Time Entry Creation MR', 'Task ID: ' + taskId);
      } catch (e) {
        log.debug('Error triggering Time Entry Creation MR', e);
      }      
      
    }

    function parseCsvLine(line) {
        // RFC-4180-ish: handles commas, quotes, and escaped quotes ("")
        var fields = [];
        var field = '';
        var inQuotes = false;

        for (var i = 0; i < line.length; i++) {
            var ch = line[i];
            if (ch === '"') {
                if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
                    field += '"'; // escaped quote
                    i++;
                } else {
                    inQuotes = !inQuotes; // toggle
                }
            } else if (ch === ',' && !inQuotes) {
                fields.push(stripOuterQuotes(field));
                field = '';
            } else {
                field += ch;
            }
        }
        fields.push(stripOuterQuotes(field));
        return fields;
    }

    function stripOuterQuotes(s) {
        if (s.length >= 2 && s[0] === '"' && s[s.length - 1] === '"') {
            s = s.substring(1, s.length - 1);
        }
        return s.replace(/""/g, '"');
    }

    function normalizeColumns(cols, expected, memoIndex) {
        // If memo wasn't quoted and got split, recombine pieces until the fixed tail size matches
        if (cols.length > expected) {
            var tailCount = expected - memoIndex - 1; // columns after Memo
            var tailStart = cols.length - tailCount;

            var head = cols.slice(0, memoIndex);
            var memoParts = cols.slice(memoIndex, tailStart);
            var tail = cols.slice(tailStart);

            var memo = memoParts.join(',');
            return head.concat([memo], tail);
        }
        // Pad missing trailing empties if needed
        while (cols.length < expected) cols.push('');
        return cols;
    }

    return {
        getInputData: getInputData,
        map: map,
        reduce: reduce,
        summarize: summarize
    };
});
