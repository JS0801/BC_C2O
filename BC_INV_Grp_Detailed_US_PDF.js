/**
* @NApiVersion 2.1
* @NScriptType Suitelet
*/
define(['N/ui/serverWidget', 'N/search', 'N/record', 'N/render', 'N/url', 'N/log', 'N/format', 'N/file'],
function (serverWidget, search, record, render, url, log, format, file) {
  
  function onRequest(context) {
    if (context.request.method !== 'GET') return;
      
    var request = context.request;
    var recID   = request.parameters.recid;
    var subID   = request.parameters.subid || 1;
    var outType = (request.parameters.type || '').toUpperCase(); 
    var custID = request.parameters.custid;
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
        search.createColumn({ name: "custcol_invoicing_category", summary: "GROUP" }),
        search.createColumn({
          name: "formulatext",
          summary: "GROUP",
          sort: search.Sort.ASC,
          formula:
            "CASE " +
            "WHEN {custcol_invoicing_category} = 'Equipment / Vehicle Rental' AND {custcol_bc_tm_time_bill} IS NOT NULL THEN {custcol_bc_tm_time_bill.employee} || ' - ' || {custcol_c2o_billing_class_override} " +
            "WHEN {custcol_invoicing_category} = 'Equipment / Vehicle Rental' AND {custcol_bc_tm_source_transaction.memo} IS NOT NULL THEN {custcol_bc_tm_source_transaction.memo} " +
            "WHEN {custcol_invoicing_category} = 'Labor' THEN {custcol_bc_tm_time_bill.employee} || ' - ' || {custcol_c2o_billing_class_override} " +
            "WHEN {custcol_invoicing_category} IN ('Materials', 'Expenses') THEN {custcol_bc_tm_source_transaction.memo} " +
            "ELSE '' END"
        }),
        search.createColumn({
          name: "formulatext1",
          summary: "GROUP",
          formula:
            "CASE " +
            "WHEN {custcol_invoicing_category} IN ('Equipment / Vehicle Rental', 'Labor') THEN NVL({custcol_bc_tm_time_bill.custcol_bc_time_type}, 'ST') " +
            "WHEN {custcol_invoicing_category} IN ('Materials', 'Expenses') THEN 'Each' " +
            "ELSE '' END"
        }),
        search.createColumn({ name: "formulanumericrates", summary: "SUM", formula: "NVL({rate},0)" }),
        search.createColumn({ name: "formulanumericratem", summary: "MAX", formula: "NVL({rate},0)" }),

        search.createColumn({ name: "quantity", summary: "SUM" }),
        search.createColumn({ name: "formulanumeric", summary: "SUM", formula: "NVL({amount},0) + NVL({taxamount},0)" }),
        search.createColumn({ name: "custrecord_cponum", join: "cseg_bc_project", summary: "GROUP", label: "PO Num" }),
        search.createColumn({ name: "cseg_bc_project", summary: "GROUP", label: "Poject Num" }),
        search.createColumn({ name: "custcol_bc_tm_billing_shift", join: "custcol_bc_tm_time_bill", summary: "GROUP" }),
        search.createColumn({ name: "custrecord_client_supervisor", join: "cseg_bc_project", summary: "GROUP", label: "Poject Man" }),
        search.createColumn({ name: "formulanumericqty", summary: "SUM", formula: "{custcol_bc_tm_source_transaction.quantity}" }),
        search.createColumn({ name: "custcol_bc_time_type", join: "custcol_bc_tm_time_bill", summary: "GROUP" }),
        search.createColumn({
          name: "formulatext4",
          summary: "GROUP",
          formula:
            "CASE WHEN {custcol_invoicing_category} IN ('Materials') THEN {custcol_bc_tm_source_transaction.mainname} " +
            "WHEN {custcol_invoicing_category} IN ('Expenses') THEN {custcol_bc_tm_source_transaction.expensecategory} ELSE '' END"
        }),
        search.createColumn({
          name: "expensecategory",
          join: "CUSTCOL_BC_TM_SOURCE_TRANSACTION",
          summary: "GROUP",
          label: "Expense Category"
        }),
        search.createColumn({
            name: "formulanumerictax",
            summary: "SUM",
            formula: "CASE WHEN {custcol_invoicing_category} IN ('Equipment / Vehicle Rental', 'Labor') THEN ABS(NVL({taxamount},0)) ELSE NVL({taxamount},0) END",
            label: "Tax Amount"
          })
      ]
    });
      
    invoiceSearchObj.run().each(function (result) {
      var category = result.getText({ name: "custcol_invoicing_category", summary: "GROUP" });
      var key = result.getValue({ name: "formulatext", summary: "GROUP" });
      var unit = result.getValue({ name: "formulatext1", summary: "GROUP" });

      var sumRate = Math.abs(parseFloat(result.getValue(invoiceSearchObj.columns[3])) || 0);
      var maxRate = Math.abs(parseFloat(result.getValue(invoiceSearchObj.columns[4])) || 0);
      var quantity = Math.abs(parseFloat(result.getValue(invoiceSearchObj.columns[5])) || 0);

      var shiftType = result.getText({ name: 'custcol_bc_tm_billing_shift', join: "custcol_bc_tm_time_bill", summary: "GROUP" }) || '';
      var timeType  = result.getText({ name: 'custcol_bc_time_type',       join: "custcol_bc_tm_time_bill", summary: "GROUP" }) || '';
      var sourceqty = parseFloat(result.getValue(invoiceSearchObj.columns[11])) || 0;
      var expCat    = result.getValue({ name: 'formulatext4', summary: "GROUP" }) || '';
      var taxAmountFromSearch = parseFloat(result.getValue(invoiceSearchObj.columns[15])) || 0;

      ponum = result.getValue(invoiceSearchObj.columns[7]) || ponum;
      var projTxt = result.getText(invoiceSearchObj.columns[8]) || '';
      if (projTxt && projectnumber.indexOf(projTxt) === -1) projectnumber.push(projTxt);
      projectmanager = result.getText(invoiceSearchObj.columns[10]) || projectmanager;

      // Calculate quantities and amounts
      var qty = (category === 'Materials' || category === 'Expenses') ? (sourceqty || 1) : quantity;
      
      // Get the total with tax from search results (column 6)
      var totalWithTax = Math.abs(parseFloat(result.getValue(invoiceSearchObj.columns[6])) || 0);
      
      var lineSubtotal, lineTax, lineTotal;

      if (category === 'Materials' || category === 'Expenses') {
        // For Materials/Expenses, sumRate is already the pre-tax total
        lineSubtotal = Math.abs(sumRate);
        lineTotal = totalWithTax;
        lineTax = taxAmountFromSearch;
      } else {
        // For Labor/Equipment: calculate pre-tax as maxRate * quantity
        lineSubtotal = Math.abs(parseFloat(maxRate) * parseFloat(qty));
        lineTotal = totalWithTax;  // Use the search result that includes tax
        lineTax = taxAmountFromSearch;
      }

      // Round everything to 2 decimal places
      lineSubtotal = Math.round((lineSubtotal + Math.sign(lineSubtotal) * 1e-8) * 100) / 100;
      lineTotal = Math.round((lineTotal + Math.sign(lineTotal) * 1e-8) * 100) / 100;
      lineTax = Math.round((lineTax + Math.sign(lineTax) * 1e-8) * 100) / 100;

      var obj = {
        description: (key || '').replace(/&/g, '&amp;'),
        unit:        (unit || '').replace(/&/g, '&amp;'),
        shiftType:   shiftType && shiftType !== "- None -" ? shiftType.replace(/&/g, '&amp;') : '',
        timeType:    timeType  && timeType  !== "- None -" ? timeType.replace(/&/g, '&amp;')  : '',
        quantity:    (category === 'Materials' || category === 'Expenses') ? (sourceqty.toFixed(1) || '1.0') : (qty.toFixed(1)),
        unitRate:    formatCurrency(maxRate),
        total:       formatCurrency(lineTotal),
        totalV:      lineTotal,
        category:    category,
        project:     projTxt,
        expCat:      expCat && expCat !== "- None -" ? expCat.replace(/&/g, '&amp;') : '',
        subtotal:    lineSubtotal,
        taxtotal:    lineTax,
        lineSubtotal:formatCurrency(lineSubtotal),
        lineTax:     formatCurrency(lineTax),
        lineTaxV:    lineTax
      };

      if (!groupedData[category]) groupedData[category] = [];
      groupedData[category].push(obj);
      return true;
    });

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
    // 1) project ASC
    var p = cmpTextAsc(x.project, y.project); 
    if (p !== 0) return p;

    // 2) description ASC
    var d = cmpTextAsc(x.description, y.description);
    if (d !== 0) return d;

    // 3) timeType by rank ASC
    var rx = timeRank(x.timeType);
    var ry = timeRank(y.timeType);
    return rx - ry;
  });
}


    // ==== build finalArray with grouping, per your PDF expectation ====
    var finalArray = [];
    var sortOrder = ['Labor', 'Equipment / Vehicle Rental', 'Materials', 'Expenses'];
    var groupTotalFinal = 0, groupSubFinal = 0, groupTaxFinal = 0, groupQtyFinal = 0;
    var projectMap = {}; // Labor project rollups

    var LABOR_TIME_ORDER = ['ST', 'OT', 'DT', 'Per Diem', 'PTO', 'Jury'];
    var LABOR_TIME_RANK = LABOR_TIME_ORDER.reduce(function (m, v, i) { m[v] = i; return m; }, {});
    function num(x){ return (typeof x === 'number') ? x : (parseFloat(String(x).replace(/[^0-9.-]/g,'')) || 0); }

    sortOrder.forEach(function (cat) {
      if (!groupedData[cat]) return;

      finalArray.push({ groupstart: cat });

      var groupTotal = 0, groupSub = 0, groupTax = 0, groupQty = 0;
      var entries = groupedData[cat];

      var lastProject = null;
      var currProjTotal = 0, currProjSub = 0, currProjTax = 0, currProjQty = 0;

      function pushProjectSummary(projectName){
        var v = projectMap[projectName] || {
          stQty:0, dtQty:0, otQty:0, pdQty:0, ptoQty:0, juryQty:0,
          stTotal:0, dtTotal:0, otTotal:0, pdTotal:0, ptoTotal:0, juryTotal:0,
          totalQty:0, total:0
        };
        finalArray.push({
          projectSummary: true,
          project: projectName,
          projectTotal: formatCurrency(currProjTotal),
          projectSub:   formatCurrency(currProjSub),
          projectTax:   formatCurrency(currProjTax),
          projectQty:   formatCurrency(currProjQty),

          stQty: v.stQty,   stTotal: formatCurrency(v.stTotal),
          otQty: v.otQty,   otTotal: formatCurrency(v.otTotal),
          dtQty: v.dtQty,   dtTotal: formatCurrency(v.dtTotal),
          pdQty: v.pdQty,   pdTotal: formatCurrency(v.pdTotal),
          ptoQty: v.ptoQty, ptoTotal: formatCurrency(v.ptoTotal),
          juryQty: v.juryQty, juryTotal: formatCurrency(v.juryTotal),

          totalQty: v.totalQty,
          total:    formatCurrency(v.total)
        });
      }

      entries.forEach(function(entry){
        if (cat === 'Labor') {
          var projectName = entry.project || '-None-';
          if (lastProject !== null && projectName !== lastProject) {
            pushProjectSummary(lastProject);
            currProjTotal = currProjSub = currProjTax = currProjQty = 0;
          }
          if (projectName !== lastProject) {
            entry.projectHeader = projectName;
            lastProject = projectName;
          } else {
            entry.projectHeader = '';
          }
        }

        var tV = num(entry.totalV), st = num(entry.subtotal), tx = num(entry.lineTaxV), qx = num(entry.quantity);
        groupTotal      += tV; groupSub += st; groupTax += tx; groupQty += qx;
        groupTotalFinal += tV; groupSubFinal += st; groupTaxFinal += tx; groupQtyFinal += qx;

        if (cat === 'Labor') {
          currProjTotal += tV; currProjSub += st; currProjTax += tx; currProjQty += qx;

          var projectName = entry.project || '-None-';
          if (!projectMap.hasOwnProperty(projectName)){
            projectMap[projectName] = {
              project: projectName,
              stQty:0, dtQty:0, otQty:0, pdQty:0, ptoQty:0, juryQty:0,
              stTotal:0, dtTotal:0, otTotal:0, pdTotal:0, ptoTotal:0, juryTotal:0,
              totalQty:0, total:0
            };
          }
          var pm = projectMap[projectName];
          var q  = num(entry.quantity);
          if (entry.timeType === 'ST') pm.stQty += q, pm.stTotal += tV-tx;
          else if (entry.timeType === 'DT') pm.dtQty += q, pm.dtTotal += tV-tx;
          else if (entry.timeType === 'OT') pm.otQty += q, pm.otTotal += tV-tx;
          else if (entry.timeType === 'Per Diem') pm.pdQty += q, pm.pdTotal += tV-tx;
          else if (entry.timeType === 'PTO') pm.ptoQty += q, pm.ptoTotal += tV-tx;
          else if (entry.timeType === 'Jury') pm.juryQty += q, pm.juryTotal += tV-tx;
          pm.totalQty += q; pm.total += tV-tx;
        }

        finalArray.push(entry);
      });

      log.debug('finalArray', finalArray)
      log.debug('projectMap', projectMap)

        if (cat === 'Labor') {
    if (lastProject !== null) {
      // Push final project's summary
      pushProjectSummary(lastProject);
    }

    // Build subtotal across ALL projects (ST/OT/DT/PD/PTO/Jury)
    var roll = {
      stQty: 0, dtQty: 0, otQty: 0, pdQty: 0, ptoQty: 0, juryQty: 0,
      stTotal: 0, dtTotal: 0, otTotal: 0, pdTotal: 0, ptoTotal: 0, juryTotal: 0,
      totalQty: 0, total: 0
    };

    Object.keys(projectMap).forEach(function (pn) {
      var v = projectMap[pn];
      roll.stQty     += v.stQty;     roll.stTotal   += v.stTotal;
      roll.otQty     += v.otQty;     roll.otTotal   += v.otTotal;
      roll.dtQty     += v.dtQty;     roll.dtTotal   += v.dtTotal;
      roll.pdQty     += v.pdQty;     roll.pdTotal   += v.pdTotal;
      roll.ptoQty    += v.ptoQty;    roll.ptoTotal  += v.ptoTotal;
      roll.juryQty   += v.juryQty;   roll.juryTotal += v.juryTotal;
      roll.totalQty  += v.totalQty;  roll.total     += v.total;
    });

    // Push Labor subtotal as a "project" summary row (LAST for Labor)
    finalArray.push({
      projectSummary: true,
      project: 'Labor Time Type Subtotal',

      // Use Labor group totals here
      projectTotal: formatCurrency(groupTotal),
      projectSub:   formatCurrency(groupSub),
      projectTax:   formatCurrency(groupTax),
      projectQty:   formatCurrency(groupQty),

      stQty: roll.stQty,     stTotal:   formatCurrency(roll.stTotal),
      otQty: roll.otQty,     otTotal:   formatCurrency(roll.otTotal),
      dtQty: roll.dtQty,     dtTotal:   formatCurrency(roll.dtTotal),
      pdQty: roll.pdQty,     pdTotal:   formatCurrency(roll.pdTotal),
      ptoQty: roll.ptoQty,   ptoTotal:  formatCurrency(roll.ptoTotal),
      juryQty: roll.juryQty, juryTotal: formatCurrency(roll.juryTotal),

      totalQty: roll.totalQty,
      total:    formatCurrency(roll.total)
    });

  }

      finalArray.push({
        group: cat,
        groupTotal: formatCurrency(groupTotal),
        groupSub:   formatCurrency(groupSub),
        groupTax:   formatCurrency(groupTax),
        groupQty:   formatCurrency(groupQty)
      });
    });

    finalArray.push({
      groupTotalFinal: formatCurrency(groupTotalFinal),
      groupSubFinal:   formatCurrency(groupSubFinal),
      groupTaxFinal:   formatCurrency(groupTaxFinal)
    });

    // Load records used in both PDF and Excel
    var invoiceGroupRec = record.load({ type: 'invoicegroup', id: recID });
    var subsidiaryRec   = record.load({ type: 'subsidiary', id: subID });
    var replaceLabor    = (subsidiaryRec.getText('country') === 'Australia');

    // Logo URL
    var logoId = subsidiaryRec.getValue('logo');
    var fileUrl = '';
    if (logoId) {
      try { fileUrl = file.load({id: logoId}).url || ''; } catch(e){ log.debug('logo load err', e); }
    }

    var contacts = getCustomerGroupContacts(custID);
    log.debug("Contacts", contacts);

    // === CSV (Excel) branch ===
    if (outType === 'CSV') {
      // local copy with Labour replacement if needed
      var excelArray = finalArray;
      if (replaceLabor) {
        excelArray.forEach(function (entry) {
          for (var k in entry) {
            if (typeof entry[k] === 'string') entry[k] = entry[k].replace(/\bLabor\b/g, 'Labour');
          }
        });
      }

      var excelHtml = buildExcelHtml({
        finalArray: excelArray,
        invoiceGroupRec: invoiceGroupRec,
        subsidiaryRec:   subsidiaryRec,
        contacts: contacts,
        ponum: ponum,
        projectmanager: projectmanager,
        projectnumber: projectnumber,
        logoUrl: (fileUrl
          ? ( (url.resolveDomain ? 'https://' + url.resolveDomain({hostType: url.HostType.APPLICATION}) : '') + fileUrl )
          : ''), // if blank, header just omits <img>
        replaceLabor: replaceLabor
      });

      var xlsFile = file.create({
        name: 'Invoice_Group_' + recID + '.xls',
        fileType: file.Type.PLAINTEXT, // HTML-as-Excel
        contents: excelHtml,
        encoding: file.Encoding.UTF_8
      });
      context.response.writeFile(xlsFile, false);
      return; // stop here (skip PDF)
    }

    // === PDF branch ===
    var renderer = render.create();
    renderer.setTemplateByScriptId('CUSTTMPL_213_9873410_337');

    var xmlTemplateFile = renderer.templateContent;

    // inject template vars expected by your XML
    xmlTemplateFile = xmlTemplateFile.replace('${contactName}', contacts.name? contacts.name.replace(/&/g, "&amp;"): '');
    xmlTemplateFile = xmlTemplateFile.replace('${contactEmail}', contacts.email? contacts.email.replace(/&/g, "&amp;"): '');
    xmlTemplateFile = xmlTemplateFile.replace('${contactPhone}', contacts.phone? contacts.phone.replace(/&/g, "&amp;"): '');
    xmlTemplateFile = xmlTemplateFile.replace('${ponum}', (ponum||'').replace(/&/g, "&amp;"));
    xmlTemplateFile = xmlTemplateFile.replace('${projectmanager}', (projectmanager||'').replace(/&/g, "&amp;"));
    xmlTemplateFile = xmlTemplateFile.replace('${projectnum}', (projectnumber.length ? projectnumber.join("<br/>") : '').replace(/&/g, "&amp;"));

    if (fileUrl) {
      // Use logged-in domain for images (same as Excel build)
      var domain = (url.resolveDomain ? 'https://' + url.resolveDomain({hostType: url.HostType.APPLICATION}) : '');
      var safeLogo = (domain + fileUrl).replace(/&/g, '&amp;');
      xmlTemplateFile = xmlTemplateFile.replace('${logoURL}', safeLogo);
    } else {
      xmlTemplateFile = xmlTemplateFile.replace('${logoURL}', ""); // or your fallback URL if desired
    }

    renderer.templateContent = xmlTemplateFile;

    if (replaceLabor) {
      finalArray.forEach(function (entry) {
        for (var key in entry) {
          if (typeof entry[key] === 'string') {
            entry[key] = entry[key].replace(/\bLabor\b/g, 'Labour');
          }
        }
      });
    }

    renderer.addCustomDataSource({
      format: render.DataSource.OBJECT,
      alias: 'item',
      data: { result: finalArray }
    });
      
    renderer.addRecord('record', invoiceGroupRec);
    renderer.addRecord('subsidiary', subsidiaryRec);
      
    var pdfFile = renderer.renderAsPdf();
    context.response.writeFile(pdfFile, true);
  }
  
  // ---------------- helpers ----------------
function formatCurrency(value) {
  let n = Number(value);
  if (!isFinite(n)) n = 0;

  // force half-up behavior (e.g., 1.125 -> 1.13, -1.125 -> -1.13)
  const rounded = Math.round((n + Math.sign(n) * 1e-8) * 100) / 100;

  const parts = rounded.toFixed(2).split('.');
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return parts.join('.');
}


  // Build Excel HTML that mirrors your PDF grouping/columns
  function buildExcelHtml(ctx) {
    function esc(s){ return (s==null?'':String(s)).replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>'); }
    function br(s){ return esc(s).replace(/\r?\n/g,'<br/>'); }
    function fmtDate(d){
      try{var t=new Date(d);if(isNaN(t))return'';var M=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][t.getMonth()];return ('0'+t.getDate()).slice(-2)+'-'+M+'-'+t.getFullYear();}catch(_){return'';}
    }
    function num(v){ if(v==null||v==='')return'0'; var x=String(v).replace(/[$,\s]/g,''); var n=parseFloat(x); return isFinite(n)?String(n):'0'; }

    var MONEY_FMT = "mso-number-format:'\\0022$\\0022\\ \\#,\\#\\#0.00'";
    var QTY_FMT   = "mso-number-format:'\\#\\#0.0'";

    var rec = ctx.invoiceGroupRec, sub = ctx.subsidiaryRec;
    var c   = ctx.contacts||{};
    var V   = {};
    V.invId        = rec && rec.id ? String(rec.id) : '';
    var trandate   = rec ? rec.getValue('trandate') : '';
    V.dateTxt      = fmtDate(trandate);
    V.billPeriod   = rec.getText('custrecord2') + " - " +  rec.getText('custrecord3');
    V.terms        = rec ? (rec.getText('terms') || '') : '';
    V.customerName = rec ? (rec.getText('entity') || rec.getText('customername') || '') : '';
    V.billAddr     = rec ? (rec.getValue('billaddress') || '') : '';
    V.memo         = rec ? (rec.getValue('memo') || '') : '';
    V.logoUrl      = ctx.logoUrl || '';

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
    V.labelLabor   = ctx.replaceLabor ? 'Labour' : 'Labor';

    var TD7  = 'border:0px solid #000;padding:6px;vertical-align:middle;font-size:12pt;';
    var TD6  = 'border:1px solid #000;padding:4px;vertical-align:middle;font-size:12pt;';
    var LEFT='text-align:left;', RIGHT='text-align:right;', CENT='text-align:center;';
    var BLUE='background-color:#3a4b87;color:#FFFFFF;font-weight:bold;';

    function groupTitleCell(txt){
      return '<tr></td><td></tr><tr></td><td></tr><tr><td style="font-weight:bold;'+TD6+'width:42%;">'+esc(txt)+'</td><td></td><td ></td><td ></td><td ></td><td ></td></tr>';
    }
    function groupHeaderRow(section){
      if(section==='Expenses'){
        return '<tr style="'+CENT+BLUE+'">'
             + '<td colspan="3" style="'+TD6+BLUE+'" bgcolor="#3a4b87">Description</td>'
             + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Supplier / Category</td>'
             + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Cost</td>'
             + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Amount with Markup</td>'
             + '</tr>';
      }
      if(section==='Materials'){
        return '<tr style="'+CENT+BLUE+'">'
             + '<td colspan="2" style="'+TD6+BLUE+'" bgcolor="#3a4b87">Description</td>'
             + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Unit</td>'
             + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Quantity</td>'
             + '<td style="'+TD6+BLUE+'" bgcolor="#3a4b87">Cost</td>'
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
    function sectionSubtotalsRow(sub, tax, tot){
      return ''
      + '<table><tr><td colspan="4" style="border:0;"></td>'
      +   '<td align="right" style="'+TD6+'border-top:0;border-right:0;"><b>SubTotal</b></td>'
      +   '<td align="right" style="'+TD6+'border-top:0;border-left:1px solid #000;'+MONEY_FMT+'"><b>'+num(sub)+'</b></td>'
      + '</tr>'
      + '<tr><td colspan="4" style="border:0;"></td>'
      +   '<td align="right" style="'+TD6+'border-top:0;border-right:0;"><b>Sales Tax</b></td>'
      +   '<td align="right" style="'+TD6+'border-top:0;border-left:1px solid #000;'+MONEY_FMT+'"><b>'+num(tax)+'</b></td>'
      + '</tr>'
      + '<tr>'
      +   '<td colspan="4" style="border:0;"></td>'
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
       + '<td style="' + TD7 + 'vertical-align:middle;" valign="middle">'
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
        +     '<td colspan="2" style="'+TD7+'border-bottom:1px solid #000;">'+esc(V.billPeriod)+'</td><td style="'+TD7+'border-bottom:1px solid #000;"></td></tr>';

    html += '<tr><td></td></tr><tr>'
          +   '<td align="right" valign="top" rowspan="3" style="'+TD7+'"><b>To:</b></td>'
          +   '<td valign="top" rowspan="3" style="'+TD7+'">'+ br(V.billAddr) +'</td><td rowspan="3" style="'+TD7+'"></td>'
          +   '<td colspan="3" align="center" style="'+TD7+'border-bottom:1px solid #000;"><b>Remittance Information</b></td>'
          + '</tr>';

    html += '<tr><td align="right" style="'+TD7+'"><b>Account Name:</b></td><td colspan="2" style="'+TD7+'">'+esc(V.accountName)+'</td></tr>';
    html += '<tr><td align="right" style="'+TD7+'"><b>Bank Name:</b></td><td colspan="2" style="'+TD7+'">'+esc(V.bankName)+'</td></tr>';

    html += '<tr><td align="right" style="'+TD7+'"><b>Customer Contact:</b></td><td style="'+TD7+'">'+esc(V.projectMgr)+'</td><td></td>'
          +     '<td align="right" style="'+TD7+'"><b>Routing No.:</b></td><td colspan="2" style="'+TD7+'">'+esc(V.routingNo)+'</td></tr>';

    html += '<tr><td align="right" style="'+TD7+'"><b>Customer Ref#:</b></td><td style="'+TD7+'">'+esc(V.customerRef)+'</td><td></td>'
          +     '<td align="right" style="'+TD7+'border-bottom:1px solid #000;"><b>Account No.:</b></td><td colspan="2" style="'+TD7+'border-bottom:1px solid #000;">'+esc(V.accountNo)+'</td></tr>';

    html += '<tr><td align="right" style="'+TD7+'"><b>Project:</b></td><td style="'+TD7+'">'+ V.projectList +'</td>'

    html += '<tr><td align="right" style="'+TD7+'"><b>Memo:</b></td><td colspan="3" style="'+TD7+'">'+esc(V.memo)+'</td></tr>';
    html += '<tr><td colspan="6" style="border-bottom:1px solid #000;"></td></tr>';
    html += '</table>';

    var current = '', open = false;
    function openSection(section){
      html += '<table border="0" cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse;font-size:12pt;margin-top:6px">'
            + '<colgroup><col style="width:40%"/><col style="width:10%"/><col style="width:10%"/><col style="width:10%"/><col style="width:10%"/><col style="width:20%"/></colgroup>';
      html += groupTitleCell(section);
      html += groupHeaderRow(section);
      open = true;
    }

    (ctx.finalArray || []).forEach(function(line){
      if(line.projectHeader && line.projectHeader !== ''){
        if(open){ html += '</table>'; open=false; }
        html += '<table border="0" cellspacing="0" cellpadding="0" style="width:100%;table-layout:fixed;border-collapse:collapse;font-size:12pt;margin-top:6px">'
             +  '<tr style="background-color:#3a4b87;color:#fff;font-weight:bold;text-align:center;">'
             +  '<td colspan="6" style="'+TD6+'">'+esc(line.projectHeader)+'</td></tr>';
      }

      if(line.groupstart){
        if(open){ html += '</table>'; open=false; }
        current = (line.groupstart==='Labor') ? V.labelLabor : line.groupstart;
        openSection(current);
        return;
      }
      if(line.groupTotal){
        html += sectionSubtotalsRow(line.groupSub, line.groupTax, line.groupTotal);
        html += '</table>'; open=false; return;
      }
      if(line.groupTotalFinal){
        if(open){ html += '</table>'; open=false; }
        html += grandSummaryBlock(line);
        return;
      }
      if(line.projectSummary){
        if(open){ html += '</table>'; open=false; }
        html += '<table border="0" cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse;font-size:12pt;margin-top:6px">';
        html += '<tr style="'+CENT+BLUE+'"><td style="'+TD6+'" colspan="6">'+esc(line.project)+'</td></tr>';
        function r(lbl, q, t){
          if(!q || String(q)==='0') return '';
          return '<tr><td style="'+TD6+'"></td><td style="'+TD6+CENT+'">'+lbl+'</td><td style="'+TD6+CENT+'"></td>'
               + '<td align="right" style="'+TD6+QTY_FMT+'">'+num(q)+'</td>'
               + '<td style="'+TD6+'"></td><td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(t)+'</td></tr>';
        }
        html += r('ST', line.stQty, line.stTotal);
        html += r('DT', line.dtQty, line.dtTotal);
        html += r('OT', line.otQty, line.otTotal);
        html += r('Per Diem', line.pdQty, line.pdTotal);
        html += r('PTO', line.ptoQty, line.ptoTotal);
        if(line.totalQty){ html += r('<b>Total</b>', line.totalQty, line.total); }
        html += '</table>';
        return;
      }


      // detail rows
      if(current==='Expenses'){
        html += '<tr>'
              +   '<td colspan="3" style="'+TD6+LEFT+'">'+esc(line.description)+'</td>'
              +   '<td style="'+TD6+CENT+'">'+esc(line.expCat||'')+'</td>'
              +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.unitRate)+'</td>'
              +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.lineSubtotal)+'</td>'
              + '</tr>';
      } else if(current==='Materials'){
        html += '<tr>'
              +   '<td colspan="2" style="'+TD6+LEFT+'">'+esc(line.description)+'</td>'
              +   '<td style="'+TD6+CENT+'">'+esc(line.unit)+'</td>'
              +   '<td align="right" style="'+TD6+QTY_FMT+'">'+num(line.quantity)+'</td>'
              +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.unitRate)+'</td>'
              +   '<td align="right" style="'+TD6+RIGHT+MONEY_FMT+'">'+num(line.lineSubtotal)+'</td>'
              + '</tr>';
      } else {
        log.debug('line', line)
        html += '<tr>'
              +   '<td style="'+TD6+LEFT+'">'+(line.description)+'</td>'
              +   '<td style="'+TD6+CENT+'">'+(line.unit||'')+'</td>'
              +   '<td style="'+TD6+CENT+'">'+(line.shiftType||'')+'</td>'
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
        return false; // continue to next result
    });

    return results;
}

  return { onRequest: onRequest };
});