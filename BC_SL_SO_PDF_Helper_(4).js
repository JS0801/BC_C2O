/**
* @NApiVersion 2.1
* @NScriptType Suitelet
*/
define(['N/ui/serverWidget', 'N/search', 'N/log', 'N/file', 'N/encode', 'N/runtime', 'N/record'], function (ui, search, log, file, encode, runtime, record) {
  
  function onRequest(context) {
    if (context.request.method === 'GET') {
      var params = context.request.parameters;
      var tranid = params.tranid;
      var replaceLabor = false;
      log.debug('tranid', tranid)
      if(!tranid) return;
      var empNameArr = getEmployeeList();
      log.debug('empNameArr', empNameArr)
      try {
        const resultsArray = [];
        const shiftSortOrder = ['ST', 'OT', 'OT1.5', 'DT', 'NT', 'RDO'];
        const salesorderSearchObj = search.create({
          type: "salesorder",
          settings: [{ name: "consolidationtype", value: "NONE" }],
          filters: [
            ["type", "anyof", "SalesOrd"],
            "AND",
          //  [["custcol_bc_tm_time_bill","noneof","@NONE@"],"OR",[["custcol_bc_tm_source_transaction","noneof","@NONE@"],"AND",["formulatext: case when {custcol_bc_tm_line_id} = {custcol_bc_tm_source_transaction.line} then 1 else 0 end","is","1"]]],
            ["custcol_bc_tm_time_bill", "noneof", "@NONE@"],
            "AND",
            ["internalid", "anyof", tranid]
          ],
          columns: [
            search.createColumn({ name: "custcol_invoicing_category", summary: "GROUP", label: "Invoicing Category" }),
            search.createColumn({ name: "employee", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP", label: "Employee" }),
            search.createColumn({ name: "durationdecimal", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "SUM", label: "Duration (Decimal)" }),
            search.createColumn({ name: "item", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP", label: "Item" }),
            search.createColumn({ name: "formulatext1", formula: "NVL(NVL(NVL({custcol_c2o_billing_class_override},{custcol_bc_tm_time_bill.custcol_bc_tm_labor_billing_class}), {custcol_bc_tm_source_transaction.memo}),'')", summary: "GROUP", label: "Labor Billing Class" }),
            search.createColumn({ name: "memo", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP", label: "Note" }),
            search.createColumn({ name: "custcol_bc_time_type", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP", label: "Time Type" }),
            search.createColumn({ name: "custcol_bc_tm_billing_shift", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP", label: "Shift" }),
            search.createColumn({ name: "date", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP", label: "Date", sort: search.Sort.ASC }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "TRIM(TO_CHAR({custcol_bc_tm_time_bill.date}, 'Day'))",
              label: "Day of Week"
            }),
            search.createColumn({
              name: "country",
              join: "subsidiary",
              summary: "GROUP",
              label: "Country"
            }),
            search.createColumn({
         name: "custcol_bc_tm_line_id",
         summary: "GROUP"
      }),
      search.createColumn({
         name: "memo",
         join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
         summary: "GROUP"
      }),
      search.createColumn({
         name: "trandate",
         join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
         summary: "GROUP"
      }),
      search.createColumn({
         name: "quantity",
         join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
         summary: "SUM"
      })
          ]
        });
        
        const employeeMap = {};
        const uniqueDates = new Set();
        
        salesorderSearchObj.run().each(function (result) {
          replaceLabor = result.getText({ name: "country", join: "subsidiary", summary: "GROUP" }) == 'Australia';
          const billRentalRole = (result.getValue({ name: "memo", join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION", summary: "GROUP" })) == '- None -'? '': result.getValue({ name: "memo", join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION", summary: "GROUP" })
          const empName = result.getValue({ name: "employee", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP" }) || 1039;
          const role = result.getValue({ name: "formulatext1", summary: "GROUP" }) == '- None -'?'': result.getValue({ name: "formulatext1", summary: "GROUP" });
          const shiftType = result.getText({ name: "custcol_bc_time_type", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP" }) || '';
          const dateStr = result.getValue({ name: "date", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP" }) || result.getValue({ name: "trandate", join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION", summary: "GROUP" });
          const hours = parseFloat(result.getValue({ name: "durationdecimal", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "SUM" })) || parseFloat(result.getValue({ name: "quantity", join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION", summary: "SUM" }));
          const note = result.getValue({ name: "memo", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP" }) || '';
          const groupType = result.getText({ name: "custcol_invoicing_category", summary: "GROUP" }) || '';
          const shift = result.getText({ name: "custcol_bc_tm_billing_shift", join: "CUSTCOL_BC_TM_TIME_BILL", summary: "GROUP" }) || '';
          
          uniqueDates.add(dateStr);
          
          const empKey = empName + '_' + shiftType + '_' + role + '_' + groupType + '_' + shift;
          if (!employeeMap[empKey]) {
            employeeMap[empKey] = {
              employee: empNameArr[empName],
              role: role || billRentalRole,
              shiftType: (shiftType == "- None -")? '': shiftType,
              shift: (shift == "- None -")? '': shift.replace(" Time", ""), 
              dateMap: {},
              totalWeek: 0,
              notes: '',
              groupType: groupType
            };
          }
          
          employeeMap[empKey].dateMap[dateStr] = hours.toFixed(1);
          employeeMap[empKey].totalWeek += hours;
          
          if (note) {
            employeeMap[empKey].notes += (employeeMap[empKey].notes ? ' | ' : '') + note;
          }
          
          return true;
        });
        
        const sortedDates = Array.from(uniqueDates).sort(function(a, b) {
          return new Date(a) - new Date(b);
        });
        
        // Grouping final output by groupType
        const groupedFinalArray = {};
        
        // Create the header row (same for all groups)
        const headerRow = {
          employee: "Name",
          role: "Role",
          shiftType: "Shift<br/>Type",
          shift: "Shift",
          days: sortedDates.map(d => ({ date: d })),
          totalWeek: "TOTAL WEEK",
          notes: "Notes",
          groupType: "Group Type"
        };
        
        // Create groups
        Object.values(employeeMap).forEach(emp => {
          const row = {
            employee: emp.employee.replace(/&/g, '&amp;'),
            role: emp.role.replace(/&/g, '&amp;'),
            shiftType: emp.shiftType.replace(/&/g, '&amp;'),
            shift: emp.shift.replace(/&/g, '&amp;'),
            days: [],
            totalWeek: emp.totalWeek.toFixed(1),
            notes: emp.notes.replace(/&/g, '&amp;'),
            groupType: emp.groupType.replace(/&/g, '&amp;')
          };
          
          sortedDates.forEach(date => {
            row.days.push({
              date: date,
              hours: emp.dateMap[date] || 0
            });
          });
          
          if (!groupedFinalArray[emp.groupType]) {
            groupedFinalArray[emp.groupType] = [headerRow]; // start with header
          }
          
          groupedFinalArray[emp.groupType].push(row);
        });
        
        Object.keys(groupedFinalArray).forEach(group => {
          const header = groupedFinalArray[group].shift(); // remove header temporarily
          
          // Sort rows
          const sortedGroup = groupedFinalArray[group].sort((a, b) => {
            const empA = a.employee.toLowerCase();
            const empB = b.employee.toLowerCase();
            if (empA !== empB) return empA < empB ? -1 : 1;
            
            const indexA = shiftSortOrder.indexOf(a.shiftType);
            const indexB = shiftSortOrder.indexOf(b.shiftType);
            return indexA - indexB;
          });
          
          // Build totals row
          const totalRow = {
            employee: "TOTAL",
            role: "",
            shiftType: "",
            shift: "",
            days: [],
            totalWeek: 0,
            notes: "",
            groupType: group
          };
          
          // Prepare totals per date
          sortedDates.forEach(date => {
            let dateSum = 0;
            sortedGroup.forEach(row => {
              const day = row.days.find(d => d.date === date);
              if (day) {
                dateSum += parseFloat(day.hours || 0)  ;
              }
            });
            totalRow.days.push({ date: date, hours: dateSum.toFixed(1) });
            totalRow.totalWeek = parseFloat(totalRow.totalWeek) + parseFloat(dateSum);
          });
          totalRow.totalWeek = parseFloat(totalRow.totalWeek).toFixed(1);
          
          // Final group with header + sorted data + total row
          groupedFinalArray[group] = [header, ...sortedGroup, totalRow];
        });
        
        //************************************************************************************
        
        const transactionSearch = search.create({
          type: "salesorder",
          settings:[{"name":"consolidationtype","value":"NONE"}],
          filters:
          [
            ["type","anyof","SalesOrd"],
            "AND",
            ["custcol_bc_tm_source_transaction","noneof","@NONE@"],
            "AND",
            ["internalid","anyof",tranid],
            "AND",
            ["formulatext: case when {custcol_bc_tm_line_id} = {custcol_bc_tm_source_transaction.line} then 1 else 0 end","is","1"]
          ],
          columns:
          [
            search.createColumn({
              name: "custcol_invoicing_category",
              summary: "GROUP",
              label: "Invoicing Category"
            }),
            search.createColumn({
              name: "tranid",
              join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
              summary: "GROUP",
              label: "Document Number"
            }),
            search.createColumn({
              name: "mainname",
              join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
              summary: "GROUP",
              label: "Main Line Name"
            }),
            search.createColumn({
              name: "amount",
              join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
              summary: "MAX",
              label: "Amount"
            }),
            search.createColumn({
              name: "taxamount",
              join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
              summary: "MAX",
              label: "Amount"
            }),
            search.createColumn({
              name: "line",
              join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
              summary: "GROUP"
            }),
            search.createColumn({
              name: "memo",
              join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
              summary: "GROUP",
              label: "Memo"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "MAX",
              formula: "CASE   WHEN {custcol_bc_tm_source_transaction.appliedtotransaction} LIKE 'Purchase Order%'   THEN TRIM(REPLACE({custcol_bc_tm_source_transaction.appliedtotransaction}, 'Purchase Order', '')) END",
              label: "Formula (Text)"
            }),
            search.createColumn({
              name: "custcol_bc_tm_line_id",
              summary: "GROUP",
              label: "T&M Billing Source Line ID"
            }),
            search.createColumn({
              name: "amount",
              summary: "MAX",
              label: "Amount"
            }),
            search.createColumn({
              name: "expensecategory",
              join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
              summary: "GROUP",
              label: "Expense Category"
            }),
            search.createColumn({
              name: "country",
              join: "subsidiary",
              summary: "GROUP",
              label: "Country"
            })
          ]
        });
        const tranFinalArray = {};
        transactionSearch.run().each(function (result) {
          replaceLabor = result.getText({ name: "country", join: "subsidiary", summary: "GROUP" }) == 'Australia'

          const invoicingCategory = result.getText({
            name: "custcol_invoicing_category",
            summary: "GROUP"
          }) || 'Uncategorized';
          
          const docNumber = result.getValue({
            name: "tranid",
            join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
            summary: "GROUP"
          });
          
          const mainName = result.getText({
            name: "mainname",
            join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
            summary: "GROUP"
          });
          
          const expCat = result.getText({
            name: "expensecategory",
            join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
            summary: "GROUP"
          });
          
          const cost = parseFloat(result.getValue({
            name: "amount",
            join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
            summary: "MAX"
          })) || 0;

          const tax = parseFloat(result.getValue({
            name: "taxamount",
            join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
            summary: "MAX"
          })) || 0;
          
          const amount = parseFloat(result.getValue({
            name: "amount",
            summary: "MAX"
          })) || 0;
          
          const memo = result.getValue({
            name: "memo",
            join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
            summary: "GROUP"
          }) || '';
          
          const cleanedPO = result.getValue({
            name: "formulatext",
            summary: "MAX"
          }) || '';
          
          const row = {
            documentNumber: docNumber.replace(/&/g, '&amp;'),
            mainName: mainName.replace(/&/g, '&amp;'),
            expCat: (expCat == '- None -'? mainName: expCat),
            amount: amount.toFixed(2),
            cost: cost.toFixed(2),
            tax: tax.toFixed(2),
            memo: memo.replace(/&/g, '&amp;').replace(/:/g, '-'),
            cleanedPO: (cleanedPO == '- None -'? '' : cleanedPO.replace(/&/g, '&amp;'))
          };
          
          if (!tranFinalArray[invoicingCategory]) {
            tranFinalArray[invoicingCategory] = {
              rows: [],
              totalAmount: 0,
              totalCost: 0,
              totaltax: 0
            };
          }
          
          tranFinalArray[invoicingCategory].rows.push(row);
          tranFinalArray[invoicingCategory].totalAmount += amount;
          tranFinalArray[invoicingCategory].totalCost += cost;
          tranFinalArray[invoicingCategory].totaltax += tax;
          
          return true;
        });
        
        Object.keys(tranFinalArray).forEach(category => {
          const group = tranFinalArray[category];
          const grpAmt = (group.totalAmount).toFixed(2)
          const grpCost = (group.totalCost).toFixed(2)
          const grpTax = (Math.abs(group.totaltax)).toFixed(2)
          const grpTotal = (Math.abs(group.totaltax) + group.totalCost).toFixed(2)
          const totalRow = {
            documentNumber: "TOTAL",
            mainName: "",
            expCat: "",
            amount: formatCurrency(grpAmt),
            cost: formatCurrency(grpCost),
            tax: formatCurrency(grpTax),
            total: formatCurrency(grpTotal),
            memo: "",
            cleanedPO: ""
          };
          groupedFinalArray[category] = [...group.rows, totalRow];
        });
        
        log.debug("Final Grouped Weekly JSON", JSON.stringify(groupedFinalArray));
        log.debug('final', groupedFinalArray)
        for (var key in groupedFinalArray){
          log.debug(key, groupedFinalArray[key])
        }
if (replaceLabor){
  Object.keys(groupedFinalArray).forEach(function(groupKey) {
  const group = groupedFinalArray[groupKey];

  // Replace the key itself if it's "Labor"
  const newKey = groupKey === 'Labor' ? 'Labour' : groupKey;


  group.forEach(function(row) {
    for (var key in row) {
      if (typeof row[key] === 'string') {
        row[key] = row[key].replace(/\bLabor\b/g, 'Labour');
      }
    }
  });
});
}

        // -------------------------------------------------------
        // BUILD TIME TYPE LEGEND from groupedFinalArray
        // -------------------------------------------------------
        var TIME_LEGEND_MAP = {
          'ST':       'Standard Time',
          'OT':       'Overtime',
          'DT':       'Double Time',
          'PT':       'Part Time',
          'PTO':      'Paid Time Off',
          'Per Diem': 'Per Diem Allowance',
          'DR1':      'Drive Time 1',
          'DR2':      'Drive Time 2',
          'DR3':      'Drive Time 3'
        };

        var seenTypes = {};
        var legendArray = [];

        // Scan groupedFinalArray for unique shiftType values
        Object.keys(groupedFinalArray).forEach(function(category) {
          var rows = groupedFinalArray[category];
          rows.forEach(function(row) {
            if (row.shiftType && row.shiftType !== '' && !seenTypes[row.shiftType]) {
              // Clean up shiftType if it has HTML
              var cleanType = row.shiftType.replace(/<br\/>/g, '').trim();
              if (TIME_LEGEND_MAP[cleanType]) {
                seenTypes[cleanType] = true;
                legendArray.push({
                  abbr: cleanType,
                  label: TIME_LEGEND_MAP[cleanType]
                });
              }
            }
          });
        });

        // Sort legend in consistent order
        var LEGEND_ORDER = ['ST', 'OT', 'DT', 'PT', 'PTO', 'Per Diem', 'DR1', 'DR2', 'DR3'];
        legendArray.sort(function(a, b) {
          return LEGEND_ORDER.indexOf(a.abbr) - LEGEND_ORDER.indexOf(b.abbr);
        });

        // Apply Labour replacement to legend if needed
        if (replaceLabor) {
          legendArray.forEach(function(item) {
            if (item.label) {
              item.label = item.label.replace(/\bLabor\b/g, 'Labour');
            }
          });
        }

        log.debug('Time Type Legend', legendArray);
        // -------------------------------------------------------

        const labelLabor = replaceLabor ? 'Labour' : 'Labor';
        if (context.request.parameters.export === 'excel') {
          try {
            const grouped = groupedFinalArray;
            const recordObj = record.load({type: 'salesorder', id: tranid}); // assuming already loaded
            const subrec = record.load({type: 'subsidiary', id: recordObj.getValue({ fieldId: 'subsidiary' })})
            var logo = subrec.getValue('logo')
            var logoUrl = '';
            if (logo) {
              var fileUrl = "https://9873410-sb1.app.netsuite.com" + file.load({id: logo}).url;
              logoUrl = fileUrl.replace(/&/g, "&amp;")
            }
            else logoUrl = "https://9873410-sb1.app.netsuite.com/core/media/media.nl?id=11486&amp;c=9873410_SB1&amp;h=1hbkOLk3U5GSjdY4GjdiGdKUZDkL4wsovPepc9ocNenvsfSW";
            log.debug('URL', logoUrl)
            // Assuming `recordObj` is loaded
            const client = recordObj.getText({ fieldId: 'entity' }) || '';
            const customerRef = recordObj.getValue({ fieldId: 'otherrefnum' }) || '';
            const weekEnding = recordObj.getText({ fieldId: 'trandate' }) || '';
            const docNumber = recordObj.getValue({ fieldId: 'tranid' }) || '';
            const description = recordObj.getValue({ fieldId: 'memo' }) || '';
            const supervisor = recordObj.getText({ fieldId: 'custbody_client_supervisor' }) || '';
            const startTime = recordObj.getText({ fieldId: 'custbody_start_time' }) || '';
            const endTime = recordObj.getText({ fieldId: 'custbody_end_time' }) || '';
            const projectId = recordObj.getValue({ fieldId: 'cseg_bc_project' }) || '';
            let projectName = '';
            let projectManager = '';
            let reportingProject = '';
            
            if (projectId) {
              try {
                const projectRec = record.load({
                  type: 'customrecord_cseg_bc_project',
                  id: projectId
                });
                reportingProject = projectRec.getText({ fieldId: 'cseg_c2o_rep_proj' }) || '';
                projectManager = projectRec.getText({ fieldId: 'custrecord_bc_proj_manager' }) || '';
                projectName = projectRec.getText({ fieldId: 'name' }) || '';
              } catch (e) {
                log.error('Project Load Failed', e.message);
              }
            }
            var x = groupedFinalArray;
            // Start building HTML
            let html = `<html xmlns:x="urn:schemas-microsoft-com:office:excel">
            <head>
            <meta charset="UTF-8">
            <style>
            table { border-collapse: collapse; width: 100%; font-size: 10pt; table-layout: fixed; }
            th, td { border: 1px solid black; padding: 5px; word-wrap: break-word; }
            th { background-color: #3a4b87; color: white; font-weight: bold; }
            .section-label { background-color: #e3e3e3; font-weight: bold; padding: 6px; border: 1px solid #000; }
            .row-label { background-color: #3a4b87; color: white; font-weight: bold; }
            .signature-block td { padding: 20px; height: 40px; }
            .info-header { background-color: #00a3e0; color: white; font-weight: bold; }
            </style>
            </head>
            <body>
            <table style="width:100%; border-collapse: collapse; font-size:10pt;">
            <tr>
            <td style="width:30%; padding: 10px;" colspan = "5" rowspan = "7">
            <img src=${logoUrl} width="297" height="105" />
            </td>
            <td rowspan = "7" style="width:40%; font-size:26pt; font-weight:bold; text-align:center; vertical-align:middle;" colspan =  "14">Weekly Timesheet</td>
            </tr>
            </table>
            <br/><br/><br/>
            <table style="width:100%; border-collapse:collapse; font-size:9pt; table-layout:fixed; margin-top:10px;">
            <tr>
            <td colspan="2" style="width:49%; vertical-align:top;">
            <table style="width:100%; border-collapse:collapse;">
            <tr>
            <td style="width:30%;" class="info-header" colspan = "2">Client:</td>
            <td style="width:70%; border:1px solid #000;" colspan = "4">${client}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">Customer Ref #:</td>
            <td style="border:1px solid #000;" colspan = "4">${customerRef}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">Week-Ending:</td>
            <td style="border:1px solid #000;" colspan = "4">${formatDateMMDDYYYY(weekEnding)}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">C2O Project Manager:</td>
            <td style="border:1px solid #000;" colspan = "4">${projectManager}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">Description of Work:</td>
            <td style="border:1px solid #000;" colspan = "4">${description}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">Document #:</td>
            <td style="border:1px solid #000;" colspan = "4">${docNumber}</td>
            </tr>
            </table>
            </td>
            
            <td style="width:2%; border: 0px;" colspan = "4"></td>
            
            <td colspan="2" style="width:49%; vertical-align:top;">
            <table style="width:100%; border-collapse:collapse;">
            <tr>
            <td style="width:30%;" class="info-header" colspan = "2">Project:</td>
            <td colspan="3" style="border:1px solid #000;" colspan = "4">${reportingProject}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">C2O Job:</td>
            <td colspan="3" style="border:1px solid #000;" colspan = "4">${projectName}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">Supervisor:</td>
            <td colspan="3" style="border:1px solid #000;" colspan = "4">${supervisor}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">Start Time: Monday – Friday</td>
            <td style="border:1px solid #000;">${startTime}</td>
            <td class="info-header" colspan = "2">Finish Time:</td>
            <td style="border:1px solid #000;">${endTime}</td>
            </tr>
            <tr>
            <td class="info-header" colspan = "2">Start Time: Weekend / Holiday</td>
            <td style="border:1px solid #000;">${startTime}</td>
            <td class="info-header" colspan = "2">Finish Time:</td>
            <td style="border:1px solid #000;">${endTime}</td>
            </tr>
            </table>
            </td>
            </tr>
            </table>
            
            <!-- LABOR SECTION -->
            <br/><br/><br/>`;
            
            if (x.Labor) {
            html += `<table>
            <tr>
            <th colspan="6">${labelLabor}</th>
            <td colspan="13" align="center" style = "border-top: 1px solid #000; border-bottom: 1px solid #000; border-left: 1px solid #000; border-right: 1px solid #000;">ALL HOURS SHOWN ARE HOURS WORKED</td>
            </tr>
            <tr>
            <th colspan="2" rowspan="2">Name</th>
            <th colspan="2" rowspan="2">Role</th>
            <th rowspan="2">Time Type</th>
            <th rowspan="2">Shift Type</th>
            <!-- Loop through Dates -->`;
            var labor = x.Labor;
            
              for (var i = 0; i < labor[0].days.length; i++) {
              var d = labor[0].days[i];
              html += `<th>${getDayName(d.date)}</th>`
            }
            html += `
            <th rowspan="2">Total Week</th>
            <th rowspan="2" colspan="${12 - labor[0].days.length}">Notes</th>
            </tr>
            <tr>
            <!-- Date row headers -->`;
            for (var i = 0; i < labor[0].days.length; i++) {
              var d = labor[0].days[i];
              html += `<th>${formatDateMMDDYYYY(d.date)}</th>`
            }
            html += `
            </tr>
            <!-- Loop through each labor entry -->`;
            for (var q = 1; q < labor.length; q++) {
              html += `<tr>
              <td colspan="2">${labor[q].employee}</td>
              <td colspan="2">${labor[q].role}</td>
              <td>${labor[q].shiftType}</td>
              <td>${labor[q].shift}</td>`;
              for (var w = 0; w < labor[q].days.length; w++) {
                var day = labor[q].days[w] || '';
                html += `<td>${day.hours}</td>`
              }
              html += `<td>${labor[q].totalWeek}</td>
              <td colspan="${12 - labor[q].days.length}"></td>
              </tr>`
            }
            html += `</table>`;
            }
            
            if (x["Equipment / Vehicle Rental"]){
              var equp = x["Equipment / Vehicle Rental"];
              
              
              html += `<br/><br/><br/>
              <table>
              <tr><th colspan="8">Equipment / Vehicle Rental</th></tr>
              <tr>
              <th colspan="4" rowspan="2">Role</th>
              <!-- Loop through Dates -->`;
              var labor = x.Labor;
              for (var i = 0; i < equp[0].days.length; i++) {
                var d = equp[0].days[i];
                html += `<th>${getDayName(d.date)}</th>`
              }
              html += `
              <th rowspan="2">Total Week</th>
              <th rowspan="2" colspan="${14 - equp[0].days.length}">Notes</th>
              </tr>
              <tr>
              <!-- Date row headers -->`;
              for (var i = 0; i < equp[0].days.length; i++) {
                var d = equp[0].days[i];
                html += `<th>${formatDateMMDDYYYY(d.date)}</th>`;
              }
              html += `
              </tr>`;
              
              
              for (var r = 1; r < equp.length; r++) {
                var row = equp[r];
                html += `<tr>
                <td colspan="4">${row.role}</td>`;
                for (var t = 0; t < row.days.length; t++) {
                  var d = row.days[t]
                  html += `<td>${d.hours}</td>`
                }
                html += `<td>${row.totalWeek}</td>
                <td colspan="${14 - row.days.length}"></td>
                </tr>`
              }
              
              html += `</table>`;
            }
            
            if (x.Materials){
              html += `<br/><br/><br/>
              <table>
              <tr><th colspan = "5">Materials</th></tr>
              <tr>
              <th colspan="2">Supplier Invoice #</th>
              <th colspan="3">Supplier</th>
              <th colspan="2">PO #</th>
              <th colspan="8">Description</th>
              <th>Total Cost excl. Tax</th>
              </tr>`;
              for (var p = 0; p < x.Materials.length; p++) {
                var m = x.Materials[p];
                if (m.documentNumber == 'TOTAL'){
                   html += `<tr>
                  <td colspan="13" style = "border:0px solid #000;"></td>
                  <td colspan="2" align="right" style = "background-color:#3a4b87; color:white; font-weight:bold;">Total</td>
                  <td align="right">${m.cost}</td>
                  </tr>`
                }else {
                html += `<tr>
                <td colspan="2">${m.documentNumber}</td>
                <td colspan="3">${m.mainName}</td>
                <td colspan="2">${m.cleanedPO}</td>
                <td colspan="8">${m.memo}</td>
                <td align="right">${m.cost}</td>
                </tr>`}
              }
              html += `</table>`;
            }
            
            if (x.Expenses) {
              html += `<br/><br/><br/>
              <table>
              <tr><th colspan = "5">Expenses</th></tr>
              <tr>
              <th colspan="5">Expense Category</th>
              <th colspan="2">PO #</th>
              <th colspan="8">Description</th>
              <th>Total Cost excl. Tax</th>
              </tr>`;
              for (var a = 0; a < x.Expenses.length; a++) {
                var e = x.Expenses[a];
                if (e.documentNumber == 'TOTAL'){
                   html += `<tr>
                   <td colspan="13" style = "border:0px solid #000;"></td>
                  <td colspan="2" align="right" style = "background-color:#3a4b87; color:white; font-weight:bold;">Total</td>
                  <td align="right">${e.cost}</td>
                  </tr>`
                }else {
                  html += `<tr>
                <td colspan="5">${e.expCat}</td>
                <td colspan="2">${e.cleanedPO}</td>
                <td colspan="8">${e.memo}</td>
                <td align="right">${e.cost}</td>
                </tr>`
                }
                  
                
              }
              html += `</table>`;
            }

            // -------------------------------------------------------
            // INSERT TIME TYPE LEGEND before signature section
            // -------------------------------------------------------
            if (legendArray.length > 0) {
              html += `
              <br/><br/>
              <table style="width:100%; border-top: 1px solid #ccc; border-collapse:collapse; font-size:9pt;">
              <tr>
                <td style="padding-top:8px; padding-bottom:8px;">
                  <strong>Time Type Legend:</strong>&nbsp;&nbsp;`;
              
              for (var lg = 0; lg < legendArray.length; lg++) {
                html += `<strong>${legendArray[lg].abbr}</strong> – ${legendArray[lg].label}`;
                if (lg < legendArray.length - 1) {
                  html += '&nbsp;&nbsp;|&nbsp;&nbsp;';
                }
              }
              
              html += `
                </td>
              </tr>
              </table>`;
            }
            // -------------------------------------------------------
            
            html += `
            <!-- SIGNATURE SECTION -->
            <br/><br/><br/>
            <table style="width:100%; border-collapse:collapse; font-size:9pt; table-layout:fixed; margin-top:10px;">
            <tr>
            <td colspan="2" style="width:49%; vertical-align:top;">
            <table style="width:100%; border-collapse:collapse;">
            <tr>
            <td style="background:#3a4b87; color:white; font-weight:bold; border:1px solid #000; text-align:center; padding:8px;" colspan="6"><b>C2O APPROVAL</b></td>
            </tr>
            <tr>
              <td style="width:20%; border: 1px solid black; background-color:#3a4b87; color:white; font-weight:bold; padding:10px; vertical-align:middle; text-align:left; line-height:0.9; font-size:9pt;" colspan="1">
                Signature:<br/>Name:<br/>Date:
              </td>
              <td style="width:80%; border: 1px solid black; height: 100px; vertical-align:top;" colspan="5"></td>
            </tr>
            </table>
            </td>
            
            <td style="width:2%; border: 0px;" colspan="4"></td>
            
            <td colspan="2" style="width:49%; vertical-align:top;">
            <table style="width:100%; border-collapse:collapse;">
            <tr>
            <td style="background:#3a4b87; color:white; font-weight:bold; border:1px solid #000; text-align:center; padding:8px;" colspan="6"><b>CLIENT APPROVAL</b></td>
            </tr>
            <tr>
              <td style="width:20%; border: 1px solid black; background-color:#3a4b87; color:white; font-weight:bold; padding:10px; vertical-align:middle; text-align:left; line-height:0.9; font-size:9pt;" colspan="1">
                Signature:<br/>Name:<br/>Date:
              </td>
              <td style="width:80%; border: 1px solid black; height: 100px; vertical-align:top;" colspan="5"></td>
            </tr>
            </table>
            </td>
            </tr>
            </table>
            </body>
            </html>`;
            
            
            // Create file and save to cabinet
            var excelFile = file.create({
              name: `Weekly_Timesheet_${new Date().toISOString().slice(0,10)}.xls`,
              fileType: file.Type.PLAINTEXT,
              contents: html,
              encoding: file.Encoding.UTF_8
            });
            
            context.response.writeFile(excelFile, false);    

            return;
            
            
          } catch (e) {
            log.error('Excel Export Error', e.message);
            context.response.write('Error generating Excel: ' + e.message);
          }
        }
        var strReturn = "<#assign ObjDetail=" + JSON.stringify(groupedFinalArray) + " />";
        log.debug('strReturn', strReturn);
        
        context.response.writeLine(strReturn);
      } catch (e) {
        log.error('Error running Suitelet', e);
        
        context.response.write('Error: ' + e.message);
      }
    }
  }

function formatDateMMDDYYYY(dateStr) {
  log.debug('dateStr', dateStr);

  var parts = dateStr.split('/'); // MM/DD/YYYY
  var m = parseInt(parts[0], 10) - 1;
  var d = parseInt(parts[1], 10);
  var yStr = parts[2];           // year as string
if (yStr.length === 2) {
  yStr = '20' + yStr;          // add 20 as prefix
}
var y = parseInt(yStr, 10);
  log.debug('y', y)

  var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  return d + '-' + months[m] + '-' + y;
}
  
  function getEmployeeList() {
    var returnObj = {}
    
    var employeeSearchObj = search.create({
      type: "employee",
      filters:[],
      columns:
      [
        search.createColumn({name: "internalid", label: "Internal ID"}),
        search.createColumn({
          name: "formulatext",
          formula: "{firstname} || ' ' || {lastname}",
          label: "Formula (Text)"
        })
      ]
    });
    var searchResultCount = employeeSearchObj.runPaged().count;
    employeeSearchObj.run().each(function(result){
      returnObj[JSON.parse(result.getValue({name: 'internalid'}))] = result.getValue({name: 'formulatext'})
      return true;
    });
    
    return returnObj;
  }
  
  function formatCurrency(amount) {
    if (isNaN(amount)) return '$ 0.00';
    return '$ ' + parseFloat(amount).toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }
  
  function getDayName(dateStr) {
    // Example input: "06/27/2025"
    var date = new Date(dateStr);
    if (isNaN(date)) return ''; // Handle invalid date
    
    var days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    return days[date.getDay()];
  }
  
  return { onRequest };
});