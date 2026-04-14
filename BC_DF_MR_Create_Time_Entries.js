/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/record', 'N/search', 'N/file', 'N/log'],
function(record, search, file, log) {

    function getInputData() {
        return search.create({
			type: 'customrecord_bc_df_ts_row_data',
			filters: [
				['custrecord_bc_df_ts_row_status', 'anyof', '1'],
				//'AND', 
				//['custrecord_bc_df_ts_row_parent', 'anyof', ['26']]
			],
			columns: [
				'internalid',
				'custrecord_bc_df_ts_row_parent',
				'custrecord_bc_df_ts_row_number',
				'custrecord_bc_df_ts_row_bc_project',
				'custrecord_bc_df_ts_row_labor_bill_code',
				'custrecord_bc_df_ts_row_service_item',
				'custrecord_bc_df_ts_row_emp_id',
				'custrecord_bc_df_ts_row_emp_name',
				'custrecord_bc_df_ts_row_date',
				'custrecord_bc_df_ts_row_shift_type',
				'custrecord_bc_df_ts_row_memo',
				'custrecord_bc_df_ts_row_duration',
				'custrecord_bc_df_ts_row_start_time',
				'custrecord_bc_df_ts_row_end_time',
				'custrecord_bc_df_ts_row_department',
				'custrecord_bc_df_ts_row_dept_class',
				'custrecord_bc_df_ts_row_billable',
				'custrecord_bc_df_ts_row_pay_category'
			]
		});
    }

    function map(context) {
        var result = JSON.parse(context.value);
        var values = result.values;

        var parentId = values.custrecord_bc_df_ts_row_parent.value;
        var rowNum = values.custrecord_bc_df_ts_row_number;

        var row = {
            internalid: result.id,
            custrecord_bc_df_ts_row_parent: parentId,
            custrecord_bc_df_ts_row_number: rowNum,
            custrecord_bc_df_ts_row_bc_project: values.custrecord_bc_df_ts_row_bc_project || '',
            custrecord_bc_df_ts_row_labor_bill_code: values.custrecord_bc_df_ts_row_labor_bill_code || '',
            custrecord_bc_df_ts_row_service_item: values.custrecord_bc_df_ts_row_service_item || '',
            custrecord_bc_df_ts_row_emp_id: values.custrecord_bc_df_ts_row_emp_id || '',
            custrecord_bc_df_ts_row_emp_name: values.custrecord_bc_df_ts_row_emp_name || '',
            custrecord_bc_df_ts_row_date: values.custrecord_bc_df_ts_row_date || '',
            custrecord_bc_df_ts_row_shift_type: values.custrecord_bc_df_ts_row_shift_type || '',
            custrecord_bc_df_ts_row_memo: values.custrecord_bc_df_ts_row_memo || '',
            custrecord_bc_df_ts_row_duration: values.custrecord_bc_df_ts_row_duration || '',
            custrecord_bc_df_ts_row_start_time: values.custrecord_bc_df_ts_row_start_time || '',
            custrecord_bc_df_ts_row_end_time: values.custrecord_bc_df_ts_row_end_time || '',
            custrecord_bc_df_ts_row_department: values.custrecord_bc_df_ts_row_department || '',
            custrecord_bc_df_ts_row_dept_class: values.custrecord_bc_df_ts_row_dept_class || '',
            custrecord_bc_df_ts_row_billable: values.custrecord_bc_df_ts_row_billable || '',
            custrecord_bc_df_ts_row_pay_category: values.custrecord_bc_df_ts_row_pay_category || ''
        };

        context.write(parentId + '_' + rowNum, JSON.stringify({
            parentId: parentId,
            rowId: row.internalid,
            rowNum: rowNum,
            rowData: row
        }));
    }

    function reduce(context) {
        var data = JSON.parse(context.values[0]);

        var parentId = data.parentId;
        var rowId = data.rowId;
        var rowNum = data.rowNum;
        var row = data.rowData;

        log.debug('reduce()', 'Processing Row ' + rowNum + ' (Parent: ' + parentId + ')');

        try {
            var logs = [];

			// Split projectCostCode into project & costCode
            var projectCostCodeRaw = row.custrecord_bc_df_ts_row_bc_project || '';
            var project = '';
            var costCode = '';

            if (projectCostCodeRaw) {
              var splitParts = projectCostCodeRaw.split('-');
              project = (splitParts[0] || '').trim();
              costCode = splitParts[1] ? splitParts[1].trim() : '';
            }

            var bcproject = '';
            var cost_code = '';

            if (project) {
              bcproject = searchInternalId('customrecord_cseg_bc_project', project);
              if (!bcproject) logs.push('Project not found: ' + project);
            }

            if (costCode) {
              cost_code = searchInternalId('customrecord_cseg_bc_cost_code', costCode);
              if (!cost_code) logs.push('Cost Code not found: ' + costCode);
            }       
          
            /*var splitParts = projectCostCode.split('-');
            var project = splitParts[0].trim();
            var costCode = splitParts[1] ? splitParts[1].trim() : '';
			var cost_code = searchInternalId('customrecord_cseg_bc_cost_code', costCode);
			if (!cost_code) logs.push('Cost Code not found: ' + costCode);*/
			
            // Lookups
            var employee = searchInternalId('employee', row.custrecord_bc_df_ts_row_emp_id);
            if (!employee) logs.push('Employee not found: ' + row.custrecord_bc_df_ts_row_emp_id);

            var item = searchInternalId('item', row.custrecord_bc_df_ts_row_service_item);
            if (!item) logs.push('Item not found: ' + row.custrecord_bc_df_ts_row_service_item);

            var hours =  parseFloat(row.custrecord_bc_df_ts_row_duration || '0')
            if(item == 2166){
              hours = 1;
            }

            /*var bcproject = searchInternalId('customrecord_cseg_bc_project', project);
            if (!bcproject) logs.push('Project not found: ' + project);*/

            var laborBillingClass = searchInternalId('customrecord_bc_tm_billing_class', row.custrecord_bc_df_ts_row_labor_bill_code);
            if (!laborBillingClass) logs.push('Labor Billing Class not found: ' + row.custrecord_bc_df_ts_row_labor_bill_code);

            var rawDepartment = row.custrecord_bc_df_ts_row_department || '';
            var dept = '';
            if (!isBlank(rawDepartment)) {
                dept = searchInternalId('department', rawDepartment);
                if (!dept) logs.push('Department not found: ' + rawDepartment);
            }


            var rawClass = row.custrecord_bc_df_ts_row_dept_class || '';
            var classId = '';
            if (!isBlank(rawClass)) {
                var transformedClass = rawClass ? rawClass.replace('_', ' : ') : '';
                if(transformedClass) {
                    classId = searchInternalId('classification', transformedClass);
                    if (!classId) logs.push('Class not found: ' + rawClass);
                }
            }

            var shiftAndTime = lookupShiftAndTimeType(
				row.custrecord_bc_df_ts_row_shift_type,      // File Shift Type (WRK)
				row.custrecord_bc_df_ts_row_pay_category     // File Pay Category (Reg)
			);
			
			var shiftType = '';
			var timeType = '';
			
			 if (!shiftAndTime.shiftType || !shiftAndTime.timeType) {
            logs.push('Shift mapping not found for Pay Code: ' + row.custrecord_bc_df_ts_row_shift_type +
                ', Pay Category: ' + row.custrecord_bc_df_ts_row_pay_category);
        }
			
			if (logs.length > 0) {
				throw new Error(logs.join('; '));
			}

            // Create Time Entry
            var timeRec = record.create({ type: 'timebill', isDynamic: true });
            timeRec.setValue('customform', 345);
            timeRec.setValue('employee', employee);
            timeRec.setValue('trandate', new Date(row.custrecord_bc_df_ts_row_date));
            timeRec.setValue('hours', hours);
            timeRec.setValue('item', item);
            timeRec.setValue('memo', row.custrecord_bc_df_ts_row_memo);
            if (bcproject) timeRec.setValue('cseg_bc_project', bcproject);
			if (cost_code) timeRec.setValue('cseg_bc_cost_code', cost_code);
            if (dept) timeRec.setValue('department', dept);
            if (classId) timeRec.setValue('class', classId);
            timeRec.setValue('custcol_bc_tm_labor_billing_class', laborBillingClass);
            timeRec.setValue('custcol_bc_tm_billing_shift', shiftAndTime.shiftType);
            timeRec.setValue('custcol_bc_time_type', shiftAndTime.timeType);
            timeRec.setValue('custcol_bc_created_via_sftp', true);

            // Billable logic
            if (row.custrecord_bc_df_ts_row_billable === 'T') {
                timeRec.setValue('custcol_bc_tm_line_non_billable', false);
            } else {
                timeRec.setValue('custcol_bc_tm_line_non_billable', true);
            }

          var subsidiaryId = timeRec.getValue({fieldId: 'subsidiary'})
            log.debug('subsidiaryId', subsidiaryId);
            if(subsidiaryId){
                var locationLookup = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: subsidiaryId,
                    columns: ['custrecord_bc_sub_location']
                });
                var locationId = (locationLookup.custrecord_bc_sub_location || []).length
                    ? locationLookup.custrecord_bc_sub_location[0].value
                    : null;
                
                if(locationId){
                    log.debug('locationId', locationId);
                    timeRec.setValue('location', locationId);
                }
            }

            var timeRecId = timeRec.save();

            // Update child row
            record.submitFields({
                type: 'customrecord_bc_df_ts_row_data',
                id: rowId,
                values: {
                    'custrecord_bc_df_ts_row_timebill_rec_id': timeRecId,
                    'custrecord_bc_df_ts_row_status': 2,
                    'custrecord_bc_df_ts_row_logs': ''
                }
            });

            log.debug('Row ' + rowNum, 'SUCCESS: Time Entry ' + timeRecId + ' created');

        } catch (e) {
            log.debug('Row ' + rowNum, 'FAILED: ' + e.message);
            record.submitFields({
                type: 'customrecord_bc_df_ts_row_data',
                id: rowId,
                values: {
                    'custrecord_bc_df_ts_row_status': 3,
                    'custrecord_bc_df_ts_row_logs': 'Row ' + rowNum + ': ' + e.message
                }
            });
            context.write(parentId, JSON.stringify({
                rowNum: rowNum,
                error: e.message
            }));
        }
    }

    function summarize(summary) {
        var parentErrors = {};

        summary.output.iterator().each(function(parentId, value) {
            var data = JSON.parse(value);

            if (!parentErrors[parentId]) parentErrors[parentId] = [];
            parentErrors[parentId].push('Row ' + data.rowNum + ': ' + data.error);
            return true;
        });

        for (var parentId in parentErrors) {
            var errors = parentErrors[parentId];
			
			 // Sort errors by row number
            errors.sort(function(a, b) {
				var numA = parseInt(a.match(/Row (\d+):/)[1], 10);
				var numB = parseInt(b.match(/Row (\d+):/)[1], 10);
				return numA - numB;
			});
			
            /*var errorLines = errors.map(function(e) {
                return 'Row ' + e.rowNum + ': ' + e.error;
            });*/

            var errorFileId = null;

            if (errors.length > 0) {
                var errorFile = file.create({
                    name: 'TimeEntry_Errors_' + parentId + '.txt',
                    fileType: file.Type.PLAINTEXT,
                    contents: errors.join('\n'),
                    folder: 1663
                });
                errorFileId = errorFile.save();
                log.debug('summarize()', 'Error file created: ' + errorFileId);
            }

            record.submitFields({
                type: 'customrecord_bc_df_ts_raw_file',
                id: parentId,
                values: {
                    custrecord_bc_df_ts_time_creation_status: errors.length > 0 ? 3 : 2,
                    custrecord_bc_df_ts_failed_rows: errors.length,
                    custrecord_bc_df_ts_logs_file: errorFileId
                }
            });

            log.debug('summarize()', 'Parent ID ' + parentId + ' updated. Errors: ' + errors.length);
        }

        log.debug('summarize()', 'Stage 3 complete');
    }

    function searchInternalId(type, nameVal) {
            if (!nameVal) return '';
            var filters;

            if (type === 'employee') {
                filters = [['externalid', 'is', nameVal]];
            } else if (type === 'department') {
                filters = [['name', 'is', nameVal]];
            } else if (type === 'classification') {
                filters = [['name', 'is', nameVal]];
            } else {
                filters = [['name', 'is', nameVal]];
            }

            var result = search.create({
                type: type,
                filters: filters,
                columns: ['internalid']
            }).run().getRange({start: 0, end: 1});

            if (result && result.length > 0) {
                return result[0].getValue({name: 'internalid'});
            }
            return '';
        }
		
		function lookupShiftAndTimeType(payCodeValue, payCategoryValue) {
			if (!payCodeValue || !payCategoryValue) {
				log.debug('Shift Mapping Skipped', 'Shift Type or Pay Category is empty.');
				return { timeType: '', shiftType: '' };
			}

			// Clean values
			payCodeValue = payCodeValue.trim();
			payCategoryValue = payCategoryValue.trim();

			var filters = [
				['custrecord_bc_df_pay_code', 'is', payCodeValue],   // File Shift Type → Pay Code
				'AND',
				['custrecord_bc_df_pay_category', 'is', payCategoryValue]    // File Pay Category → Pay Category
			];

			var result = search.create({
				type: 'customrecord_bc_df_shift',
				filters: filters,
				columns: [
					'custrecord_bc_ns_time_type', // Time Type
					'custrecord_bc_ns_shift_type'   // Shift Type
				]
			}).run().getRange({ start: 0, end: 1 });

			if (result.length) {
				log.debug('Shift Mapping Found', 'Shift Type: ' + payCodeValue + ', Pay Category: ' + payCategoryValue);
				
				var timeTypeId = result[0].getValue('custrecord_bc_ns_time_type') || '';
				var shiftTypeId = result[0].getValue('custrecord_bc_ns_shift_type') || '';
				
				log.debug('Shift Mapping Found', 'TimeType ID: ' + timeTypeId + ', ShiftType ID: ' + shiftTypeId);
				
				return {
					timeType: timeTypeId,
					shiftType: shiftTypeId
				};
			}

			log.debug('Shift Mapping Not Found', 'Shift Type: ' + payCodeValue + ', Pay Category: ' + payCategoryValue);
			return { timeType: '', shiftType: '' };
		}

    function isBlank(v) { return v == null || String(v).trim() === ''; }

    return {
        getInputData: getInputData,
        map: map,
        reduce: reduce,
        summarize: summarize
    };
});
