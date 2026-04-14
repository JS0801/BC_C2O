/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */

define(['N/record', 'N/search', 'N/runtime', 'N/file', 'N/log', 'N/task'],
    function (record, search, runtime, file, log, task) {

        function getInputData() {
            try {
                //var fileId = runtime.getCurrentScript().getParameter({name: 'custscript_ts_test_file_id'});
                /*var fileId = 8933;
                if (!fileId) {
                    log.debug('Missing file');
                    return;
                }
                var testFile = file.load({id: fileId});
                var contents = testFile.getContents();

                log.debug('testFile', testFile);

                return [{
                    filename: testFile.name,
                    content: contents,
                    fileId: fileId,
                    region: 'USA'
                }];*/

                //update july 8 2025
                var fileSearch = search.create({
                    type: 'file',
                    filters: [
                        ['folder', 'anyof', '1666', '1667']
                    ],
                    columns: ['internalid', 'name', 'folder']
                });
                var results = fileSearch.run().getRange({ start: 0, end: 1000 });
                log.debug('Files Found in Folders 1666 & 1667', results.length);

                var inputArray = results.map(function (result) {
                    return {
                        filename: result.getValue('name'),
                        fileId: result.getValue('internalid'),
                        folder: result.getValue('folder'),
                        region: (result.getValue('folder') === '1666' ? 'USA' : 'AUS')
                    };
                });

                return inputArray;                

            } catch (e) {
                log.debug('Error in getInputData', e)
            }
        }

        function map(context) {
            var fileObj = JSON.parse(context.value);

            log.debug('fileObj', fileObj);

            var fileName = fileObj.filename;
            var region = fileObj.region;
            var content = fileObj.content;
            var fileId = fileObj.fileId;

            try {

                var searchExistingTsFile = search.create({
                    type: 'customrecord_bc_df_ts_raw_file',
                    filters: [['custrecord_bc_df_ts_file', 'is', fileId]],
                    columns: ['internalid']
                }).run().getRange({start: 0, end: 1});

                if (searchExistingTsFile.length == 0) {
                    var tsRawFileRec = record.create({type: 'customrecord_bc_df_ts_raw_file'});
                    tsRawFileRec.setValue('altname', fileName);
                    tsRawFileRec.setValue('custrecord_bc_df_ts_file', fileId);
                    tsRawFileRec.setValue('custrecord_bc_df_ts_status', 1); //Pending
                    tsRawFileRec.setValue('custrecord_bc_df_ts_failed_rows', 0);
                    tsRawFileRec.setValue('custrecord_bc_df_ts_total_rows', 0);
                    tsRawFileRec.setValue('custrecord_bc_df_ts_time_creation_status', 1); //Pending
                    var tsRawFileRecId = tsRawFileRec.save();
                    log.debug('Raw TS file created', 'ID: ' + tsRawFileRecId);
                } else {
                    log.debug('Raw TS already exists');
                }

            } catch (e) {
                log.debug('Error in map()', e.message);
            }
        }

        function reduce(context) {
        }

        function summarize(summary) {
            log.debug('Script 1 Complete', 'Parent record creation stage complete.');
          try {
        var mrTask = task.create({
            taskType: task.TaskType.MAP_REDUCE,
            scriptId: 'customscript_bc_df_mr_create_ts_row',
            deploymentId: 'customdeploy_bc_df_mr_create_ts_row'
        });

        var taskId = mrTask.submit();
        log.debug('Triggered MR Script', 'Task ID: ' + taskId);

    } catch (e) {
        log.debug('Error triggering second MR script', e);
    }
        }

        return {getInputData: getInputData, map: map, reduce: reduce, summarize: summarize};
    });
