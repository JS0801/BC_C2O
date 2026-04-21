/**
 * @NApiVersion 2.1
 * @NScriptType ClientScript
 */
define(['N/currentRecord', 'N/search', 'N/url'], function (currentRecord, search, url) {

    function saveRecord(context) {
        try {
            var rec = currentRecord.get();

            var currentRecordId = rec.id || '';
            var isEnabled = rec.getValue({
                fieldId: 'custrecord_bc_dayrate_enabled'
            });

            // If record is not enabled, do nothing
            if (isEnabled !== true && isEnabled !== 'T') {
                return true;
            }

            var projectId = rec.getValue({
                fieldId: 'custrecord_bc_dayrate_project'
            });

            var thresholdValue = rec.getValue({
                fieldId: 'custrecord_bc_dayrate_threshold'
            });

            var errors = [];
            var duplicateRecordId = '';
            var duplicateRecordUrl = '';

            // Threshold validation
            if (
                thresholdValue === '' ||
                thresholdValue === null ||
                thresholdValue === undefined ||
                Number(thresholdValue) <= 0
            ) {
                errors.push('Day Rate Threshold must be greater than 0.');
            }

            // Duplicate project configuration validation
            if (projectId) {
                var filters = [
                    ['custrecord_bc_dayrate_project', 'anyof', projectId],
                    'AND',
                    ['custrecord_bc_dayrate_enabled', 'is', 'T'],
                    'AND',
                    ['isinactive', 'is', 'F']
                ];

                // Exclude current record while editing
                if (currentRecordId) {
                    filters.push('AND');
                    filters.push(['internalid', 'noneof', currentRecordId]);
                }

                var duplicateSearch = search.create({
                    type: 'customrecord_c2o_dayrate_config',
                    filters: filters,
                    columns: [
                        search.createColumn({ name: 'internalid' }),
                        search.createColumn({ name: 'custrecord_bc_dayrate_project' })
                    ]
                });

                var resultSet = duplicateSearch.run().getRange({
                    start: 0,
                    end: 1
                });

                if (resultSet && resultSet.length > 0) {
                    duplicateRecordId = resultSet[0].getValue({
                        name: 'internalid'
                    });

                    duplicateRecordUrl = url.resolveRecord({
                        recordType: 'customrecord_c2o_dayrate_config',
                        recordId: duplicateRecordId,
                        isEditMode: false
                    });

                    errors.push('A Day Rate Configuration record already exists for this Project.');
                }
            }

            if (errors.length > 0) {
                var message = 'Please fix the following:\n\n- ' + errors.join('\n- ');

                if (duplicateRecordUrl) {
                    message += '\n\nClick OK to open the existing configuration record.';
                    var openRecord = window.confirm(message);

                    if (openRecord) {
                        window.open(duplicateRecordUrl, '_blank');
                    }
                } else {
                    window.alert(message);
                }

                return false;
            }

            return true;

        } catch (e) {
            console.log('saveRecord error', e);
            window.alert('Unexpected error while validating record: ' + e.message);
            return false;
        }
    }

    return {
        saveRecord: saveRecord
    };
});
