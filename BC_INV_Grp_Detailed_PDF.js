/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 * 
 * MODIFICATIONS:
 * - Fixed tax calculation for Labor/Equipment lines to use totalWithTax from search
 * - Corrected flipped subtotal and tax amounts
 * - Updated Excel export to properly display GST column
 * - Added dynamic time type legend to PDF output
 */
 define(['N/ui/serverWidget', 'N/search', 'N/record', 'N/render', 'N/url', 'N/log', 'N/format', 'N/file'],
 function (serverWidget, search, record, render, url, log, format, file) {

   function onRequest(context) {
     if (context.request.method === 'GET') {

         var request = context.request;
         var recID = request.parameters.recid;
         var subID = request.parameters.subid || 1;
         var custID = request.parameters.custid;
         var outType = (context.request.parameters.type || '').toUpperCase(); // e.g. CSV
         var projectnumber = [];
         var projectmanager = '';
         var ponum = '';
         var groupedData = {};

      var invoiceSearchObj = search.create({
        type: "invoice",
        settings: [{ name: "consolidationtype", value: "NONE" }],
        filters: [
          ["type", "anyof", "CustInvc"],
          "AND",
          ["groupedto", "anyof", recID],
          "AND",
          [
            ["custcol_bc_tm_time_bill", "noneof", "@NONE@"],
            "OR",
            [
              "formulatext: case when {custcol_bc_tm_line_id} = {custcol_bc_tm_source_transaction.line} then 1 else 0 end",
              "is",
              "1"
            ]
          ]
        ],
        columns: [
          search.createColumn({
            name: "custcol_invoicing_category",
            summary: "GROUP"
          }),
          search.createColumn({
            name: "formulatext",
            summary: "GROUP",
            sort: search.Sort.ASC,
            formula: "CASE WHEN {custcol_invoicing_category} = 'Equipment / Vehicle Rental' AND {custcol_bc_tm_time_bill} IS NOT NULL THEN {custcol_bc_tm_time_bill.employee} || ' - ' || {custcol_c2o_billing_class_override} WHEN {custcol_invoicing_category} = 'Equipment / Vehicle Rental' AND {custcol_bc_tm_source_transaction.memo} IS NOT NULL THEN {custcol_bc_tm_source_transaction.memo} WHEN {custcol_invoicing_category} = 'Labor' AND {custcol_bc_tm_source_transaction.memo} IS NULL THEN {custcol_bc_tm_time_bill.employee} || ' - ' || {custcol_c2o_billing_class_override} WHEN {custcol_invoicing_category} = 'Labor' AND {custcol_bc_tm_source_transaction.memo} IS NOT NULL THEN {custcol_bc_tm_source_transaction.memo} WHEN {custcol_invoicing_category} IN ('Materials', 'Expenses') THEN {custcol_bc_tm_source_transaction.memo} ELSE '' END"
          }),
          search.createColumn({
            name: "formulatext1",
            summary: "GROUP",
            formula: "CASE WHEN {custcol_invoicing_category} IN ('Equipment / Vehicle Rental', 'Labor') THEN NVL({custcol_bc_tm_time_bill.custcol_bc_time_type}, 'ST') WHEN {custcol_invoicing_category} IN ('Materials', 'Expenses') THEN 'Each' ELSE '' END"
          }),
          search.createColumn({ name: "formulanumericrates", summary: "SUM", formula: "NVL({rate},0)" }),
          search.createColumn({ name: "formulanumericratem", summary: "MAX", formula: "NVL({rate},0)" }),
          search.createColumn({
            name: "quantity",
            summary: "SUM"
          }),
          search.createColumn({
            name: "formulanumeric",
            summary: "SUM",
            formula: "CASE WHEN {custcol_invoicing_category} IN ('Equipment / Vehicle Rental', 'Labor') THEN ABS(NVL({amount},0)) + ABS(NVL({taxamount},0)) ELSE (NVL({amount},0)) + NVL({taxamount},0) END"
          }),
          search.createColumn({
              name: "custrecord_cponum",
              join: "cseg_bc_project",
              summary: "GROUP",
              label: "PO Num"
          }),
          search.createColumn({
              name: "cseg_bc_project",
              summary: "GROUP",
              label: "Poject Num"
          }),
          search.createColumn({
            name: "custcol_bc_tm_billing_shift",
            join: "custcol_bc_tm_time_bill",
            summary: "GROUP"
          }),
          
          search.createColumn({
              name: "custrecord_client_supervisor",
              join: "cseg_bc_project",
              summary: "GROUP",
              label: "Poject Man"
          }),
          search.createColumn({
            name: "formulanumericqty",
            summary: "SUM",
            formula: "{custcol_bc_tm_source_transaction.quantity}"
          }),
          search.createColumn({
            name: "custcol_bc_time_type",
            join: "custcol_bc_tm_time_bill",
            summary: "GROUP",
            sort: search.Sort.DESC,
          }),
          search.createColumn({
            name: "formulatext4",
            summary: "GROUP",
            formula: "CASE WHEN {custcol_invoicing_category} IN ('Materials') THEN {custcol_bc_tm_source_transaction.mainname} WHEN {custcol_invoicing_category} IN ('Expenses') THEN {custcol_bc_tm_source_transaction.expensecategory} ELSE '' END"
          }),
          search.createColumn({
           name: "expensecategory",
           join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
           summary: "GROUP",
           label: "Expense Category"
          }),
          // Add separate tax amount column
          search.createColumn({
            name: "formulanumerictax",
            summary: "SUM",
            formula: "CASE WHEN {custcol_invoicing_category} IN ('Equipment / Vehicle Rental', 'Labor') THEN ABS(NVL({taxamount},0)) ELSE NVL({taxamount},0) END",
            label: "Tax Amount"
          }),
          search.createColumn({
            name: "formulanumericcost",
            summary: "SUM",
            formula: "CASE WHEN {custcol_invoicing_category} IN ('Equipment / Vehicle Rental', 'Labor') THEN ABS(NVL({rate},0)) ELSE NVL({custcol_bc_tm_source_transaction.amount},0) END",
            label: "Tax Amount"
          })
        ]
      });
       invoiceSearchObj.run().each(function (result) {
        log.debug('result', result)
        var category = result.getText({ name: "custcol_invoicing_category", summary: "GROUP" });
        var key = result.getValue({ name: "formulatext", summary: "GROUP" });
        var unit = result.getValue({ name: "formulatext1", summary: "GROUP" });
        var sumRate = parseFloat(result.getValue(invoiceSearchObj.columns[3])) || 0;
        var maxRate = parseFloat(result.getValue(invoiceSearchObj.columns[4])) || 0;
        if (category == 'Equipment / Vehicle Rental' || category == 'Labor'){
          sumRate = Math.abs(sumRate);
          maxRate = Math.abs(maxRate);
        }
        var quantity = Math.abs(parseFloat(result.getValue(invoiceSearchObj.columns[5])) || 0);
        var shiftType = result.getText({ name: 'custcol_bc_tm_billing_shift', join: "custcol_bc_tm_time_bill", summary: "GROUP" });
        var timeType = result.getText({ name: 'custcol_bc_time_type', join: "custcol_bc_tm_time_bill", summary: "GROUP" });
        var sourceqty =  parseFloat(result.getValue(invoiceSearchObj.columns[11])) || 0;
        var expCat = result.getValue({ name: 'formulatext4', summary: "GROUP" });
        // Get the tax amount from the new column
        var taxAmountFromSearch = parseFloat(result.getValue(invoiceSearchObj.columns[15])) || 0;
        var maincost = parseFloat(result.getValue(invoiceSearchObj.columns[16])) || 0;
        
        ponum = result.getValue(invoiceSearchObj.columns[7]);
        if (projectnumber.indexOf(result.getText(invoiceSearchObj.columns[8])) == -1)
        projectnumber.push(result.getText(invoiceSearchObj.columns[8]));
        projectmanager = result.getText(invoiceSearchObj.columns[10]);
        
        // Calculate quantities and amounts
        var qty = (category === 'Materials' || category === 'Expenses') ? (sourceqty || 1) : quantity;
        
        // Get the total WITH tax from search results (column 6) - this is the correct total including GST
        var totalWithTax = Math.abs(parseFloat(result.getValue(invoiceSearchObj.columns[6])) || 0);
        
        var lineSubtotalCalc, lineTaxCalc, total;

        if (category === 'Materials' || category === 'Expenses') {
          // For Materials/Expenses, sumRate is already the pre-tax total
          lineSubtotalCalc = Math.abs(sumRate);
          total = totalWithTax;
          lineTaxCalc = taxAmountFromSearch;
        }  else {
           // For Labor/Equipment: derive subtotal from search totals instead of rate * qty
           lineSubtotalCalc = Math.abs(parseFloat(maxRate) * parseFloat(qty));
           lineTaxCalc = taxAmountFromSearch;
           total = lineSubtotalCalc + lineTaxCalc;
        }
        // else {
        //   // For Labor/Equipment: calculate pre-tax as maxRate * quantity
        //   lineSubtotalCalc = Math.abs(parseFloat(maxRate) * parseFloat(qty));
        //   total = totalWithTax;  // Use the search result that includes tax
        //   lineTaxCalc = taxAmountFromSearch; // Use the tax from search
        // }

        // Round everything to 2 decimal places
        lineSubtotalCalc = Math.round((lineSubtotalCalc + Math.sign(lineSubtotalCalc) * 1e-8) * 100) / 100;
        total = Math.round((total + Math.sign(total) * 1e-8) * 100) / 100;
        lineTaxCalc = Math.round((lineTaxCalc + Math.sign(lineTaxCalc) * 1e-8) * 100) / 100;

        log.debug('Tax Amount from Search', { category: category, taxAmountFromSearch: taxAmountFromSearch, total: total, lineSubtotalCalc: lineSubtotalCalc, lineTaxCalc: lineTaxCalc });

        var finalRate = maincost;
        
        var obj = {
          description: key.replace(/&/g, '&amp;'),
          unit: unit.replace(/&/g, '&amp;'),
          shiftType: shiftType == "- None -"? '': shiftType.replace(/&/g, '&amp;'),
          timeType: timeType == "- None -"? '': timeType.replace(/&/g, '&amp;'),
          quantity: (category === 'Materials' || category === 'Expenses') ? (sourceqty.toFixed(1) || 1): quantity.toFixed(1),
          unitRate: formatCurrency(finalRate),
          total: formatCurrency(total),
          totalV: total,
          category: category,
          expCat: expCat == "- None -"? '': expCat.replace(/&/g, '&amp;'),
          subtotal: lineSubtotalCalc,
          taxtotal: lineTaxCalc,
          // Line-level tax fields for display
          lineSubtotal: formatCurrency(lineSubtotalCalc),
          lineTax: formatCurrency(lineTaxCalc),
          lineTaxV: lineTaxCalc
        };

        log.debug('obj with tax', obj)

        if (!groupedData[category]) groupedData[category] = [];
        groupedData[category].push(obj);

        return true;
      });

       // Custom order for timeType
var TIME_ORDER = ['ST', 'DT', 'OT', 'Per Diem', 'PTO', 'Jury'];

function timeRank(tt) {
  if (!tt || tt === '- None -') return 999; // push blanks to end
  for (var i = 0; i < TIME_ORDER.length; i++) {
    if (TIME_ORDER[i] === tt) return i;
  }
  return 998; // unknowns just before blanks
}

function cmpTextAsc(a, b) {
  var sa = (a || '').toString();
  var sb = (b || '').toString();
  if (sa < sb) return -1;
  if (sa > sb) return 1;
  return 0;
}

// Sort each category: primary by timeType order, secondary by description ASC
for (var cat in groupedData) {
  if (!groupedData.hasOwnProperty(cat)) continue;
  var arr = groupedData[cat];
  if (!arr || !arr.sort) continue;

  arr.sort(function (x, y) {
    // 1️⃣ First: description ASC
    var d = cmpTextAsc(x.description, y.description);
    if (d !== 0) return d;

    // 2️⃣ Then: timeType order
    var rx = timeRank(x.timeType);
    var ry = timeRank(y.timeType);
    return rx - ry;
  });
}

      var finalArray = [];

      var sortOrder = ['Labor', 'Equipment / Vehicle Rental', 'Materials', 'Expenses'];
      var groupTotalFinal = 0;
      var groupSubFinal = 0;
      var groupTaxFinal = 0;

    sortOrder.forEach(function (cat) {
  if (groupedData[cat]) {
    finalArray.push({ groupstart: cat });

    var groupTotal = 0;
    var groupSub = 0;
    var groupTax = 0;

    groupedData[cat].forEach(function (entry) {
      finalArray.push(entry);
      groupTotal += entry.totalV;
      groupSub += entry.subtotal;
      groupTax += entry.taxtotal;
      groupTotalFinal += entry.totalV;
      groupSubFinal += entry.subtotal;
      groupTaxFinal += entry.taxtotal;
    });
    log.debug('groupTax', groupTax)
    if (groupTax < 1) {
      groupSub = groupTotal;
      groupTax = 0;
    }

    finalArray.push({
      group: cat,
      groupTotal: formatCurrency(groupTotal), 
      groupSub:   formatCurrency(groupSub),
      groupTax:   formatCurrency(groupTax)
    });
  }
});

    if (groupTaxFinal < 1) {
      groupSubFinal = groupTotalFinal;
      groupTaxFinal = 0;
    }

    finalArray.push({
      groupTotalFinal: formatCurrency(groupTotalFinal), 
      groupSubFinal:  formatCurrency(groupSubFinal),
      groupTaxFinal:  formatCurrency(groupTaxFinal) 
    });

      // -------------------------------------------------------
      // BUILD TIME TYPE LEGEND
      // Collect unique timeType values that appear in line items
      // and map them to their full descriptions.
      // -------------------------------------------------------
      var TIME_LEGEND_MAP = {
        'ST':       'Standard Time',
        'OT':       'Overtime',
        'DT':       'Double Time',
        'PT':       'Part Time',
        'PTO':      'Paid Time Off',
        'Per Diem': 'Per Diem Allowance',
        'DR1':      'Day Rate 1',
        'DR2':      'Day Rate 2',
        'DR3':      'Day Rate 3'
      };

      var seenTypes = {};
      var legendArray = [];

      finalArray.forEach(function(entry) {
        if (entry.timeType && entry.timeType !== '' && !seenTypes[entry.timeType]) {
          if (TIME_LEGEND_MAP[entry.timeType]) {
            seenTypes[entry.timeType] = true;
            legendArray.push({
              abbr:  entry.timeType,
              label: TIME_LEGEND_MAP[entry.timeType]
            });
          }
        }
      });

      // Sort legend entries in a consistent, logical order
      var LEGEND_ORDER = ['ST', 'OT', 'DT', 'PT', 'PTO', 'Per Diem', 'DR1', 'DR2', 'DR3'];
      legendArray.sort(function(a, b) {
        return LEGEND_ORDER.indexOf(a.abbr) - LEGEND_ORDER.indexOf(b.abbr);
      });

      log.debug('Legend Array', legendArray);
      // -------------------------------------------------------

      // Load Invoice Group Record (if needed)
       var invoiceGroupRec = record.load({
         type: 'invoicegroup',
         id: recID
       });

       // Load Subsidiary (if needed)
       var subsidiaryRec = record.load({
         type: 'subsidiary',
         id: subID
       });
       var replaceLabor = subsidiaryRec.getText('country') == 'Australia';

       var logo = subsidiaryRec.getValue('logo')
       var fileUrl = '';
       if (logo) {
         var fileUrl = file.load({id: logo}).url;
         log.debug('fileUrl', fileUrl)
        }

       var contacts = getCustomerGroupContacts(custID);
       log.debug("Contacts", contacts);
       
if (outType === 'CSV') {
  var excelHtml = buildExcelHtml({
    finalArray: finalArray,
    invoiceGroupRec: invoiceGroupRec,
    subsidiaryRec: subsidiaryRec,
    subID: subID,
    contacts: contacts,
    ponum: ponum,
    projectmanager: projectmanager,
    projectnumber: projectnumber,
    logoUrl: (fileUrl ? "https://9873410.app.netsuite.com" + fileUrl : "https://9873410-sb1.app.netsuite.com/core/media/media.nl?id=6602&c=9873410_SB1&h=R3DKmSPNysAlWsoMjrDKoblKq2Yc6K5CjcGxmqIAS72zqumQ"),
    replaceLabor: replaceLabor
  });

  var xlsFile = file.create({
    name: 'Invoice_Group_' + recID + '.xls',
    fileType: file.Type.PLAINTEXT,
    contents: excelHtml,
    encoding: file.Encoding.UTF_8
  });

  context.response.writeFile(xlsFile, false);
  return;
}

       // Render PDF
       var renderer = render.create();
       renderer.setTemplateByScriptId('CUSTTMPL_204_9873410_SB1_274');
       
       var xmlTemplateFile = renderer.templateContent;

         xmlTemplateFile = xmlTemplateFile.replace('${contactName}', contacts.name? contacts.name.replace(/&/g, "&amp;"): '');
         xmlTemplateFile = xmlTemplateFile.replace('${contactEmail}', contacts.email? contacts.email.replace(/&/g, "&amp;"): '');
         xmlTemplateFile = xmlTemplateFile.replace('${contactPhone}', contacts.phone? contacts.phone.replace(/&/g, "&amp;"): '');
       
         xmlTemplateFile = xmlTemplateFile.replace('${ponum}', ponum? ponum.replace(/&/g, "&amp;"): '');
         xmlTemplateFile = xmlTemplateFile.replace('${projectmanager}', projectmanager? projectmanager.replace(/&/g, "&amp;"): '');
         xmlTemplateFile = xmlTemplateFile.replace('${projectnum}', projectnumber.length? (projectnumber.join("<br/>")).replace(/&/g, "&amp;"): '');
         if (fileUrl) xmlTemplateFile = xmlTemplateFile.replace('${logoURL}', fileUrl.replace(/&/g, "&amp;"));
         else xmlTemplateFile = xmlTemplateFile.replace('${logoURL}', "https://9873410-sb1.app.netsuite.com/core/media/media.nl?id=6602&amp;c=9873410_SB1&amp;h=R3DKmSPNysAlWsoMjrDKoblKq2Yc6K5CjcGxmqIAS72zqumQ");
       
       renderer.templateContent = xmlTemplateFile;

       // Apply Labour spelling for Australian subsidiaries
       if (replaceLabor) {
         finalArray.forEach(function (entry) {
           for (var key in entry) {
             if (typeof entry[key] === 'string') {
               entry[key] = entry[key].replace(/\bLabor\b/g, 'Labour');
             }
           }
         });
         // Also apply to legend labels if any contain "Labor"
         legendArray.forEach(function (entry) {
           for (var key in entry) {
             if (typeof entry[key] === 'string') {
               entry[key] = entry[key].replace(/\bLabor\b/g, 'Labour');
             }
           }
         });
       }

       renderer.addCustomDataSource({
         format:render.DataSource.OBJECT,
         alias: 'item',
         data: {result: finalArray}
       });

       // Pass the legend array to the template
       renderer.addCustomDataSource({
         format: render.DataSource.OBJECT,
         alias: 'legend',
         data: { result: legendArray }
       });

       renderer.addRecord('record', invoiceGroupRec);
       renderer.addRecord('subsidiary', subsidiaryRec);

       var pdfFile = renderer.renderAsPdf();
       context.response.writeFile(pdfFile, true);

      log.debug('Grouped and Finalized Data', JSON.stringify(finalArray, null, 2));
    }
  }

function formatCurrency(value) {
  let n = Number(value);
  if (!isFinite(n)) n = 0;

  const rounded = Math.round((n + Math.sign(n) * 1e-8) * 100) / 100;

  const parts = rounded.toFixed(2).split('.');
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return parts.join('.');
}

function getCustomerGroupContacts(customerId) {
    var results = {};

    var customerSearchObj = search.create({
        type: "customer",
        filters: [
            ["internalid", "anyof", customerId],
            "AND",
            ["custentity_bc_c2o_group_contact", "noneof", "@NONE@"]
        ],
        columns: [
            search.createColumn({
                name: "entityid",
                join: "CUSTENTITY_BC_C2O_GROUP_CONTACT",
                label: "Name"
            }),
            search.createColumn({
                name: "email",
                join: "CUSTENTITY_BC_C2O_GROUP_CONTACT",
                label: "Email"
            }),
            search.createColumn({
                name: "homephone",
                join: "CUSTENTITY_BC_C2O_GROUP_CONTACT",
                label: "Home Phone"
            })
        ]
    });

    customerSearchObj.run().each(function (result) {
        results = {
            name: result.getValue({ name: "entityid", join: "CUSTENTITY_BC_C2O_GROUP_CONTACT" }),
            email: result.getValue({ name: "email", join: "CUSTENTITY_BC_C2O_GROUP_CONTACT" }),
            phone: result.getValue({ name: "homephone", join: "CUSTENTITY_BC_C2O_GROUP_CONTACT" })
        };
        return false;
    });

    return results;
}

function buildExcelHtml(ctx) {
  // ---------- helpers ----------
  function esc(s){ return (s==null?'':String(s)).replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>'); }
  function br(s){ return esc(s).replace(/\r?\n/g,'<br/>'); }
  function fmtDate(d){
    try{var t=new Date(d);if(isNaN(t))return'';var m=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][t.getMonth()];return ('0'+t.getDate()).slice(-2)+'-'+m+'-'+t.getFullYear();}catch(_){return'';}
  }
  function billPer(d){
    try{var t=new Date(d);if(isNaN(t))return'';var m=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][t.getMonth()];return m+'-'+String(t.getFullYear()).slice(2);}catch(_){return'';}
  }
  function num(v){ if(v==null||v==='')return'0'; var x=String(v).replace(/[$,\s]/g,''); var n=parseFloat(x); return isFinite(n)?String(n):'0'; }

  var MONEY_FMT = "mso-number-format:'\\0022$\\0022\\ \\#,\\#\\#0.00'";
  var QTY_FMT   = "mso-number-format:'\\#\\#0.0'";

  var rec   = ctx.invoiceGroupRec||null;
  var sub   = ctx.subsidiaryRec||null;
  var c     = ctx.contacts||{};
  var V     = {};

  V.invId        = rec && rec.id ? String(rec.id) : '';
  var trandate   = rec ? rec.getValue('trandate') : '';
  V.dateTxt      = fmtDate(trandate);
  V.billPeriod   = rec.getText('custrecord2') + " - " +  rec.getText('custrecord3');
  V.terms        = rec ? (rec.getText('terms') || '') : '';
  V.customerName = rec ? (rec.getText('entity') || rec.getText('customername') || '') : '';
  V.billAddr     = rec ? (rec.getValue('billaddress') || '') : '';
  V.memo         = rec ? (rec.getValue('memo') || '') : '';
  
  V.subID        = sub && sub.id ? String(sub.id) : '';
  V.subAddr      = sub ? (sub.getValue('mainaddress_text') || '') : '';
  V.accountName  = sub ? (sub.getValue('custrecord_bc_account_name') || '') : '';
  V.bankName     = sub ? (sub.getValue('custrecord_bc_bank') || '') : '';
  V.routingNo    = sub ? (sub.getValue('custrecord_bc_bsb') || '') : '';
  V.accountNo    = sub ? (sub.getValue('custrecord_bc_acc_num') || '') : '';

  V.contactName  = rec.getValue('custrecord_bc_customer_contact')  || '';
  V.c2oSuper     = rec.getValue('custrecord_bc_c2o_supervisor')  || '';
  V.contactEmail = c.email || '';
  V.contactPhone = c.phone || '';

  V.projectMgr   = ctx.projectmanager || '';
  V.customerRef  = rec.getValue('custrecord_cust_ref') || ctx.ponum || '';
  V.projectList  = (ctx.projectnumber || []).join('<br/>');
  V.logoUrl      = ctx.logoUrl || '';
  V.labelLabor   = ctx.replaceLabor ? 'Labour' : 'Labor';

  var TD7  = 'border:0px solid #000;padding:6px;vertical-align:middle;font-size:12pt;';
  var TD6  = 'border:1px solid #000;padding:4px;vertical-align:middle;font-size:12pt;';
  var LEFT = 'text-align:left;', RIGHT='text-align:right;', CENT='text-align:center;';
  var BLUE = 'background-color:#3a4b87;color:#FFFFFF;font-weight:bold;';

  function groupTitleCell(txt){
    return '<tr></td><td></tr><tr></td><td></tr><tr><td style="font-weight:bold;'+TD6+'width:42%;">'+esc(txt)+'</td><td></td><td ></td><td ></td><td ></td><td ></td></tr>';
  }
  function groupHeaderRow(section){
    if(section==='Expenses'){
      return '<tr style="'+CENT+BLUE+'">'
           + '<td colspan="3" style="'+TD6+BLUE+'" bgcolor="#3a4b87">Description</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Supplier / Category</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Cost</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">GST</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Amount with Markup</td>'
           + '</tr>';
    }
    if(section==='Materials'){
      return '<tr style="'+CENT+BLUE+'">'
           + '<td colspan="2" style="'+TD6+BLUE+'" bgcolor="#3a4b87">Description</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Unit</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Quantity</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Cost</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">GST</td>'
           + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Amount with Markup</td>'
           + '</tr>';
    }
    return '<tr style="'+CENT+BLUE+'">'
         + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Description</td>'
         + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Time Type</td>'
         + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Shift Type</td>'
         + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Quantity</td>'
         + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Unit Rate</td>'
         + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Total Amount</td>'
         + '</tr>';
  }
  function sectionSubtotalsRow(sub, tax, tot, grp){
    return ''
    + '<tr><td colspan="'+ grp +'" style="border:0;"></td>'
    +   '<td align="right" style="'+TD6+'border-top:0;border-right:0;"><b>SubTotal</b></td>'
    +   '<td align="right" style="'+TD6+'border-top:0;border-left:1px solid #000;'+MONEY_FMT+'"><b>'+num(sub)+'</b></td>'
    + '</tr>'
    + '<tr><td colspan="'+ grp +'" style="border:0;"></td>'
    +   '<td align="right" style="'+TD6+'border-top:0;border-right:0;"><b>Sales Tax</b></td>'
    +   '<td align="right" style="'+TD6+'border-top:0;border-left:1px solid #000;'+MONEY_FMT+'"><b>'+num(tax)+'</b></td>'
    + '</tr>'
    + '<tr>'
    +   '<td colspan="'+ grp +'" style="border:0;"></td>'
    +   '<td align="right" style="'+TD6+'background-color:#3a4b87;color:#fff;border-top:0;border-left:1px solid #000;border-right:0;border-bottom:1px solid #000;"><b>Total</b></td>'
    +   '<td align="right" style="'+TD6+'background-color:#3a4b87;color:#fff;border-top:0;border-left:1px solid #000;border-right:1px solid #000;border-bottom:1px solid #000;'+MONEY_FMT+'"><b>'+num(tot)+'</b></td>'
    + '</tr>';
  }
  function grandSummaryBlock(line){
    return ''
    + '<table border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;width:100%;table-layout:fixed;font-size:12pt;margin-top:6px">'
    +   '<tr><td></td></tr><tr><td></td></tr><tr><td colspan="4" align="center" rowspan="4" style="'+TD7+'font-size: 14px; "><b>Payment Terms: </b>'+esc(V.terms)+'</td>'
    +       '<td colspan="2" align="center" style="'+TD7+' border: 1px; background-color:#3a4b87;color:#FFFFFF;" bgcolor="#3a4b87"><b>Total Summary</b></td></tr>'
    +   '<tr>'
    +       '<td align="right" style="'+TD7+'border: 1px; border-top:0;border-right:0;"><b>SubTotal</b></td>'
    +       '<td align="right" style="'+TD7+' border: 1px; border-top:0;border-left:1px solid #000;'+MONEY_FMT+'"><b>'+num(line.groupSubFinal)+'</b></td></tr>'
    +   '<tr>'
    +       '<td align="right" style="'+TD7+'border: 1px; border-top:0;border-right:0;"><b>Sales Tax</b></td>'
    +       '<td align="right" style="'+TD7+'border: 1px; border-top:0;border-left:1px solid #000;'+MONEY_FMT+'"><b>'+num(line.groupTaxFinal)+'</b></td></tr>'
    +   '<tr>'
    +       '<td align="right" style="'+TD7+' border: 1px; background-color:#3a4b87;color:#fff;"><b>Total</b></td>'
    +       '<td align="right" style="'+TD7+' border: 1px; background-color:#3a4b87;color:#fff;'+MONEY_FMT+'"><b>'+num(line.groupTotalFinal)+'</b></td></tr>'
    + '</table>';
  }

  var html = '';
  html += '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">';
  html += '<html xmlns:x="urn:schemas-microsoft-com:office:excel"><head><meta charset="UTF-8"></head><body>';

  html += '<table border="0" cellspacing="0" cellpadding="0" style="width:100%;table-layout:fixed;border-collapse:collapse;font-size:12pt;">'
        + '<colgroup>'
        + '<col style="width:22%"/><col style="width:42%"/><col style="width:42%"/><col style="width:42%"/><col style="width:42%"/><col style="width:42%"/>'
        + '</colgroup>';

html += '<tr style="height:110px;">'
   +   '<td style="'+TD7+'vertical-align:middle;" valign="middle">'
   +      (V.logoUrl 
              ? '<img src="'+esc(V.logoUrl)+'" ' 
                   + (V.subID == 6 
                        ? 'style="width:110px;height:110px;"' 
                        : 'width = "250" height = "100"') 
                   + '>' 
              : '')
   +   '</td>'
   +   '<td colspan="3" style="'+TD7+'vertical-align:middle;" valign="middle">'+ br(V.subAddr) +'</td>'
   +   '<td colspan="2" style="'+TD7+'vertical-align:middle;text-align:right;font-size:18pt;font-weight:bold;" valign="middle">INVOICE #'+esc(V.invId)+'</td>'
   + '</tr>'
   + '<tr><td colspan="6" style="border-bottom:1px solid #000;"></td></tr>';

  html += '<tr><td></td></tr><tr><td align="right" style="'+TD7+'"><b>C2O Group Contact:</b></td>'
        +     '<td  style="'+TD7+'">'+esc(V.contactName)+'</td><td style="'+TD7+'"></td>'
        +     '<td align="right" style="'+TD7+'"><b>Date:</b></td>'
        +     '<td colspan="2" style="'+TD7+'">'+esc(V.dateTxt)+'</td></tr>';

  html += '<tr><td align="right" style="'+TD7+'"><b>Contact Email:</b></td>'
        +     '<td style="'+TD7+'">'+esc(V.contactEmail)+'</td><td style="'+TD7+'"></td>'
        +     '<td align="right" style="'+TD7+'"><b>Invoice No.:</b></td>'
        +     '<td colspan="2" style="'+TD7+'">'+esc(V.invId)+'</td></tr>';

  html += '<tr><td align="right" style="'+TD7+'"><b>Contact Phone:</b></td>'
        +     '<td style="'+TD7+'">'+esc(V.contactPhone)+'</td><td style="'+TD7+'"></td>'
        +     '<td align="right" style="'+TD7+'"><b>Customer:</b></td>'
        +     '<td colspan="2" style="'+TD7+'">'+esc(V.customerName)+'</td></tr>';

    html += '<tr><td align="right" style="'+TD7+'"><b>C2O Supervisor:</b></td>'
        +     '<td style="'+TD7+'">'+esc(V.c2oSuper)+'</td><td style="'+TD7+'"></td>'
        +     '<td align="right" style="'+TD7+'"></td>'
        +     '<td colspan="2" style="'+TD7+'"></td></tr>';

  html += '<tr><td align="right" style="'+TD7+'border-bottom:1px solid #000;"><b>Payment Terms:</b></td>'
        +     '<td style="'+TD7+'border-bottom:1px solid #000;">'+esc(V.terms)+'</td><td style="'+TD7+'border-bottom:1px solid #000;"></td>'
        +     '<td align="right" style="'+TD7+'border-bottom:1px solid #000;"><b>Billing Period:</b></td>'
        +     '<td colspan="2" style="'+TD7+'border-bottom:1px solid #000;">'+esc(V.billPeriod)+'</td></tr>';

  html += '<tr><td></td></tr><tr>'
        +   '<td align="right" valign="top" rowspan="3" style="'+TD7+'"><b>To:</b></td>'
        +   '<td  valign="top" rowspan="3" style="'+TD7+'">'+ br(V.billAddr) +'</td><td rowspan="3" style="'+TD7+'"></td>'
        +   '<td colspan="3" align="center" style="'+TD7+'border-bottom:1px solid #000;"><b>Remittance Information</b></td>'
        + '</tr>';

  html += '<tr>'
        +   '<td align="right" style="'+TD7+'"><b>Account Name:</b></td>'
        +   '<td colspan="2" style="'+TD7+'">'+esc(V.accountName)+'</td>'
        + '</tr>';

  html += '<tr>'
        +   '<td align="right" style="'+TD7+'"><b>Bank Name:</b></td>'
        +   '<td colspan="2" style="'+TD7+'">'+esc(V.bankName)+'</td>'
        + '</tr>';

  html += '<tr>'
        +   '<td align="right" style="'+TD7+'"><b>Customer Contact:</b></td>'
        +   '<td  style="'+TD7+'">'+esc(V.projectMgr)+'</td><td></td>'
        +   '<td align="right" style="'+TD7+'"><b>Routing No.:</b></td>'
        +   '<td colspan="2" style="'+TD7+'">'+esc(V.routingNo)+'</td>'
        + '</tr>';

  html += '<tr>'
        +   '<td align="right" style="'+TD7+'"><b>Customer Ref#:</b></td>'
        +   '<td  style="'+TD7+'">'+esc(V.customerRef)+'</td><td></td>'
        +   '<td align="right" style="'+TD7+'"><b>Account No.:</b></td>'
        +   '<td colspan="2" style="'+TD7+'">'+esc(V.accountNo)+'</td>'
        + '</tr>';

  html += '<tr>'
        +   '<td align="right" style="'+TD7+'"><b>Project:</b></td>'
        +   '<td  style="'+TD7+'">'+ V.projectList +'</td><td></td>'
        +   '<td colspan="3" align="center" style="'+TD7+'border-bottom:1px solid #000;">For the account of '+esc(V.accountName)+'</td>'
        + '</tr>';

  html += '<tr>'
        +   '<td align="right" style="'+TD7+'"><b>Memo:</b></td>'
        +   '<td colspan="4" style="'+TD7+'">'+esc(V.memo)+'</td>'
        + '</tr>';

    html += '<tr>'
        +   '<td align="right" style="'+TD7+'"><b>C2O Supervisor:</b></td>'
        +   '<td style="'+TD7+'">'+esc(V.c2oSuper)+'</td>'
        + '</tr>';

  html += '<tr><td colspan="6" style="border-bottom:1px solid #000;"></td></tr>';
  html += '</table>';

  var current = '';
  var open = false;

  function openSection(section){
    html += '<table border="0" cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse;font-size:12pt;margin-top:6px">'
          + '<colgroup><col style="width:40%"/><col style="width:10%"/><col style="width:10%"/><col style="width:10%"/><col style="width:10%"/><col style="width:20%"/></colgroup>';
    html += groupTitleCell(section);
    html += groupHeaderRow(section);
    open = true;
  }

  log.debug('finalArray', ctx.finalArray)

var laborTotal = 0;
var laborTotalV = 0;
var lineSubtotal = 0;
var inLabor = false;

for (var i = 0; i < ctx.finalArray.length; i++) {
    var row = ctx.finalArray[i];

    if (row.groupstart === 'Labor') {
        inLabor = true;
        continue;
    }

    if (inLabor && (row.groupstart || row.groupend)) {
        break;
    }

    if (inLabor && row.totalV) {
        // Strip commas from currency-formatted strings before parsing
        var parsedTotal = parseFloat((row.total || '0').toString().replace(/,/g, ''));
        var parsedLineSub = parseFloat((row.lineSubtotal || '0').toString().replace(/,/g, ''));
        var numTotalV = row.totalV;

        laborTotalV += numTotalV;
        laborTotal += parsedTotal;
        lineSubtotal += parsedLineSub;

        // Log only mismatched rows
        if (parsedTotal !== numTotalV || parsedTotal !== parsedLineSub || numTotalV !== parsedLineSub) {
            log.debug('MISMATCH at index ' + i, 
                'description: ' + row.description +
                ' | total: ' + parsedTotal +
                ' | totalV: ' + numTotalV +
                ' | lineSubtotal: ' + parsedLineSub
            );
        }
    }
}

log.debug('Labor Totals', 
    'total: ' + laborTotal.toFixed(2) +
    ' | totalV: ' + laborTotalV.toFixed(2) +
    ' | lineSubtotal: ' + lineSubtotal.toFixed(2)
);

  (ctx.finalArray || []).forEach(function(line){
    if(line.groupstart){
      if(open){ html += '</table>'; open=false; }
      current = (line.groupstart==='Labor') ? V.labelLabor : line.groupstart;
      openSection(current);
      return;
    }
    if(line.groupTotal){
      var spancol = 5;
      if (line.group == 'Labor' || line.group == 'Labour' || line.group == 'Equipment / Vehicle Rental') spancol = 4;
      html += sectionSubtotalsRow(line.groupSub, line.groupTax, line.groupTotal, spancol);
      html += '</table>';
      open=false;
      return;
    }
    if(line.groupTotalFinal){
      if(open){ html += '</table>'; open=false; }
      html += grandSummaryBlock(line);
      return;
    }

    if(current==='Expenses'){
      html += '<tr>'
            +   '<td colspan="3" style="'+TD6+LEFT+'">'+esc(line.description)+'</td>'
            +   '<td style="'+TD6+CENT+'">'+esc(line.expCat||'')+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.unitRate)+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.lineTax)+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.lineSubtotal)+'</td>'
            + '</tr>';
    } else if(current==='Materials'){
      html += '<tr>'
            +   '<td colspan="2" style="'+TD6+LEFT+'">'+esc(line.description)+'</td>'
            +   '<td style="'+TD6+CENT+'">'+esc(line.unit)+'</td>'
            +   '<td align="right" style="'+TD6+QTY_FMT+'">'+num(line.quantity)+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.unitRate)+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.lineTax)+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.lineSubtotal)+'</td>'
            + '</tr>';
    } else {
      html += '<tr>'
            +   '<td style="'+TD6+LEFT+'">'+esc(line.description)+'</td>'
            +   '<td style="'+TD6+CENT+'">'+esc(line.timeType||'')+'</td>'
            +   '<td style="'+TD6+CENT+'">'+esc(line.shiftType||'')+'</td>'
            +   '<td align="right" style="'+TD6+QTY_FMT+'">'+num(line.quantity)+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.unitRate)+'</td>'
            +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.lineSubtotal)+'</td>'
            + '</tr>';
    }
  });

  if(open){ html += '</table>'; }

  html += '</body></html>';
  return html;
}

  return {
    onRequest: onRequest
  };
});