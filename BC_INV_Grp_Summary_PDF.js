/**
* @NApiVersion 2.1
* @NScriptType Suitelet
*/
define(['N/ui/serverWidget', 'N/search', 'N/record', 'N/render', 'N/url', 'N/log', 'N/format', 'N/file'],
function (serverWidget, search, record, render, url, log, format, file) {

  function onRequest(context) {
    if (context.request.method === 'GET') {
      try {
        var request = context.request;
        var recID = request.parameters.recid;
        var subID = request.parameters.subid || 1;
        log.debug('recID', recID)
        var summaryObj = [];
        var invoiceGroupId = null;
        var subsidiaryId = null;
        var poNum = '';
        var customerRef = '';
        var projectMan = '';
        var subtotal = 0;
        var totalMain = 0;
        var totalTax = 0;
        var totalRetention = 0;
        var retentionGroups = {};
        var retentionGroupOrder = [];

        // Run search - ADD CUSTOMER FIELDS
        var invoiceSearch = search.create({
          type: "invoice",
          settings: [{ name: "consolidationtype", value: "NONE" }],
          filters: [
            ["type", "anyof", "CustInvc"],
            "AND",
            ["groupedto", "anyof", recID],
            "AND",
            ["custcol_invoicing_category", "noneof", "@NONE@"]
          ],
          columns: [
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{custcol_invoicing_category} || '_' || {taxitem}",
              label: "Key"
            }),
            search.createColumn({
              name: "custcol_invoicing_category",
              summary: "GROUP",
              label: "Invoicing Category"
            }),
            search.createColumn({
              name: "formulanumericamt",
              formula: "{amount}",
              summary: "SUM",
              label: "Amount"
            }),
            search.createColumn({
              name: "rate",
              join: "taxItem",
              summary: "GROUP",
              label: "Tax Rate"
            }),
            search.createColumn({
              name: "custrecord_cponum",
              join: "cseg_bc_project",
              summary: "MAX",
              label: "PO Num"
            }),
            search.createColumn({
              name: "custrecord_client_supervisor",
              join: "cseg_bc_project",
              summary: "MAX",
              label: "Project Manager"
            }),
            search.createColumn({
              name: "formulanumeric",
              summary: "SUM",
              formula: "NVL({amount},0)",
              label: "Total Amount"
            }),
            search.createColumn({
              name: "formulanumerictax",
              summary: "SUM",
              formula: "NVL({taxamount},0)",
              label: "Total Amount"
            }),
            search.createColumn({
              name: "custcol_bc_sov_unbilled_retention",
              summary: "SUM",
              label: "Retention Amount"
            }),
            search.createColumn({
              name: "custcol_bc_retentions_percentage",
              summary: "MAX",
              label: "Retention Percent"
            }),
            search.createColumn({
              name: "taxrate1",
              summary: "MAX",
              label: "Line Tax Rate"
            }),
            // ADD CUSTOMER NAME AND ID FIELDS
            search.createColumn({
              name: "companyname",
              join: "customer",
              summary: "MAX",
              label: "Customer Name"
            }),
            search.createColumn({
              name: "internalid",
              join: "customer",
              summary: "MAX",
              label: "Customer ID"
            }),
            // ADD CUSTOMER REF FIELD
            search.createColumn({
              name: "otherrefnum",
              summary: "MAX",
              label: "Customer Ref"
            })
          ]
        });

        var customerName = 'Invoice Group Customer';
        var customerAddress = '';
        var customerABN = '';
        var customerId = null;
        
        log.debug('About to load records');
        
        // Load Invoice Group Record FIRST to get PO and Customer Ref with priority
        var invoiceGroupRec = record.load({
          type: 'invoicegroup',
          id: recID
        });
        log.debug('Loaded invoice group record');

        var subsidiaryRec = record.load({
          type: 'subsidiary',
          id: subID
        });
        var isAustraliaSubsidiary = subsidiaryRec.getText('country') == 'Australia';
        log.debug('Loaded subsidiary record', {
          subID: subID,
          isAustraliaSubsidiary: isAustraliaSubsidiary
        });
        
        // Get PO Number and Customer Ref directly from Invoice Group FIRST (these take priority)
        try {
          var poNumFromGroup = invoiceGroupRec.getValue('ponumber');
          if (poNumFromGroup) poNum = poNumFromGroup.replace(/&/g, '&amp;');
        } catch (e) {
          log.debug('Could not get ponumber from invoice group', e);
        }
        
        try {
          var custRefFromGroup = invoiceGroupRec.getValue('custrecord_cust_ref');
          if (custRefFromGroup) customerRef = custRefFromGroup.replace(/&/g, '&amp;');
        } catch (e) {
          log.debug('Could not get custrecord_cust_ref from invoice group', e);
        }
        
        log.debug('PO and Customer Ref from Invoice Group', {poNum: poNum, customerRef: customerRef});
        
        invoiceSearch.run().each(function (result) {
          log.debug('result', result)
          var key = result.getValue({ name: 'formulatext', summary: 'GROUP' });
          var category = result.getText({ name: 'custcol_invoicing_category', summary: 'GROUP' });
          var rate = result.getValue({ name: 'taxrate1', summary: 'MAX' }) || result.getValue({ name: 'rate', join: 'taxItem', summary: 'GROUP' });
          var unitPrice = result.getValue({ name: 'formulanumericamt', summary: 'SUM' });
          var taxrate = result.getValue({ name: 'formulanumerictax', summary: 'SUM' });
          var total = result.getValue({ name: 'formulanumeric', summary: 'SUM' });
          var retentionRaw = result.getValue({ name: 'custcol_bc_sov_unbilled_retention', summary: 'SUM' });
          
          // Get PO Number and Customer Ref from search results as FALLBACK only
          var poNumRaw = result.getValue({name: "custrecord_cponum", join: "cseg_bc_project", summary: "MAX"}) || '';
          var customerRefRaw = result.getValue({name: "otherrefnum", summary: "MAX"}) || '';
          var projectManRaw = result.getValue({name: "custrecord_client_supervisor", join: "cseg_bc_project", summary: "MAX"}) || '';
          
          // Only use search values if we don't already have them from the invoice group
          if (!poNum && poNumRaw) poNum = poNumRaw.replace(/&/g, '&amp;');
          if (!customerRef && customerRefRaw) customerRef = customerRefRaw.replace(/&/g, '&amp;');
          projectMan = projectManRaw.replace(/&/g, '&amp;');
          
          // GET CUSTOMER NAME AND ID FROM SEARCH RESULTS
          var custName = result.getValue({name: "companyname", join: "customer", summary: "MAX"}) || '';
          var custId = result.getValue({name: "internalid", join: "customer", summary: "MAX"}) || '';
          
          if (custName) customerName = custName;
          if (custId && !customerId) customerId = custId;

          var lineAmount = parseAmount(unitPrice);
          var retentionAmount = isAustraliaSubsidiary ? Math.abs(parseAmount(retentionRaw)) : 0;
          var displayLineAmount = lineAmount + retentionAmount;
          
          // Calculate GST amount - try multiple approaches
          var gstAmount = parseAmount(taxrate);
          
          // If GST amount is zero, calculate it from the net post-retention line amount.
          // Retention is shown as a deduction line and does not carry GST itself.
          if (gstAmount <= 0 && rate) {
            var taxRate = parsePercentValue(rate) / 100;
            gstAmount = lineAmount * taxRate;
          }
          
          log.debug('GST Calculation', {
            total: total,
            unitPrice: unitPrice, 
            retentionAmount: retentionAmount,
            displayLineAmount: displayLineAmount,
            gstAmount: gstAmount,
            rate: rate
          });
          
          summaryObj.push({
            key: escapeXml(key || ''),
            category: escapeXml(category || ''),
            rate: rate ? formatPercent(rate) : '10.00%',
            unitPrice: "$" + formatCurrency(displayLineAmount),
            gstAmount: "$" + formatCurrency(Math.abs(gstAmount)), // Use Math.abs to ensure positive
            total: "$" + formatCurrency(displayLineAmount + gstAmount)
          })
          subtotal += lineAmount;
          totalTax += gstAmount;
          totalMain += lineAmount + gstAmount;
          return true;
        });

        if (isAustraliaSubsidiary) {
          var retentionSearch = search.create({
            type: "invoice",
            settings: [{ name: "consolidationtype", value: "NONE" }],
            filters: [
              ["type", "anyof", "CustInvc"],
              "AND",
              ["groupedto", "anyof", recID],
              "AND",
              ["custcol_invoicing_category", "noneof", "@NONE@"]
            ],
            columns: [
              search.createColumn({
                name: "custcol_bc_retentions_percentage",
                summary: "GROUP",
                label: "Retention Percent"
              }),
              search.createColumn({
                name: "custcol_bc_sov_unbilled_retention",
                summary: "SUM",
                label: "Retention Amount"
              })
            ]
          });

          retentionSearch.run().each(function (result) {
            var retentionAmount = Math.abs(parseAmount(result.getValue({
              name: "custcol_bc_sov_unbilled_retention",
              summary: "SUM"
            })));
            var retentionPercent = formatPercent(result.getValue({
              name: "custcol_bc_retentions_percentage",
              summary: "GROUP"
            }));

            if (retentionAmount > 0) {
              addRetentionGroup(retentionGroups, retentionGroupOrder, retentionPercent, retentionAmount);
              totalRetention += retentionAmount;
            }

            return true;
          });
        }

        // NOW LOAD CUSTOMER RECORD TO GET ADDRESS
        if (customerId) {
          try {
            var customerRec = record.load({
              type: 'customer',
              id: customerId
            });
            
            // Try different address fields and get ABN
            var addr1 = customerRec.getValue('billaddr1') || '';
            var addr2 = customerRec.getValue('billaddr2') || '';
            var city = customerRec.getValue('billcity') || '';
            var state = customerRec.getValue('billstate') || '';
            var zip = customerRec.getValue('billzip') || '';
            customerABN = customerRec.getValue('custentity_bc_abn') || '';
            
            // Build address string with proper line breaks
            var addressParts = [];
            if (addr1) addressParts.push(addr1);
            if (addr2) addressParts.push(addr2);
            if (city || state || zip) {
              var cityStateZip = [];
              if (city) cityStateZip.push(city);
              if (state) cityStateZip.push(state);
              if (zip) cityStateZip.push(zip);
              addressParts.push(cityStateZip.join(' '));
            }
            
            if (addressParts.length > 0) {
              customerAddress = addressParts.join('\n');
            }
            
            log.debug('Customer address loaded', customerAddress);
            
          } catch (e) {
            log.debug('Could not load customer record for address', e);
            customerAddress = 'Address not available';
          }
        }
        log.debug('summaryObj', summaryObj)
        
        log.debug('Final PO and Customer Ref values', {poNum: poNum, customerRef: customerRef});
        // Render PDF
        var renderer = render.create();
        renderer.setTemplateByScriptId('CUSTTMPL_203_9873410_SB1_835');
        
        var xmlTemplateFile = renderer.templateContent;
        log.debug('Got template content');

        // Get logo URL first
        var logoURL = '';
        var logo = subsidiaryRec.getValue('logo')
        if (logo) {
          logoURL = file.load({id: logo}).url.replace(/&/g, "&amp;");
        } else {
          logoURL = "https://9873410-sb1.app.netsuite.com/core/media/media.nl?id=6602&amp;c=9873410_SB1&amp;h=R3DKmSPNysAlWsoMjrDKoblKq2Yc6K5CjcGxmqIAS72zqumQ";          
        }
        log.debug('logoURL', logoURL);

        // GET CUSTOMER NAME AND ADDRESS
        var invoiceDate = '';
        var groupInvoiceNumber = recID;
        var memoField = '';

        try {
          // Get other invoice group data
          try {
            var tranid = invoiceGroupRec.getValue('tranid');
            if (tranid) groupInvoiceNumber = tranid;
          } catch (e) {
            log.debug('Could not get tranid', e);
          }

          try {
            var trandate = invoiceGroupRec.getValue('trandate');
            if (trandate) {
              invoiceDate = format.format({
                value: trandate,
                type: format.Type.DATE
              });
            }
          } catch (e) {
            log.debug('Could not get trandate', e);
            invoiceDate = new Date().toLocaleDateString();
          }

          // Get memo field
          try {
            var memo = invoiceGroupRec.getValue('memo');
            if (memo) memoField = memo;
          } catch (e) {
            log.debug('Could not get memo', e);
          }

        } catch (e) {
          log.error('Error getting header data', e);
        }

        // Clean up the variables to prevent XML issues but preserve line breaks
        customerName = customerName.toString().replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        customerAddress = customerAddress.toString().replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\n/g, '<br/>');
        customerABN = customerABN.toString().replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        groupInvoiceNumber = groupInvoiceNumber.toString().replace(/&/g, '&amp;');
        memoField = memoField.toString().replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

        log.debug('Header data prepared', {customerName: customerName, customerAddress: customerAddress, groupInvoiceNumber: groupInvoiceNumber, poNum: poNum, customerRef: customerRef});

        // Get subsidiary info for comparison
        var subsidiaryName = subsidiaryRec.getValue('legalname') || '';
        
        // Check subsidiary ID and set logo size accordingly
        var currentSubsidiaryId = subsidiaryRec.getValue('internalid') || subID.toString();
        var logoStyle = 'width: 100%; height: 100%; object-fit: contain;'; // default style
        var logoContainerStyle = 'width: 60px; height: 20px; overflow: hidden; position: relative; top: -15px; right: -0px;'; // default container
        var logoCSS = '';

        // Adjust logo positioning based on subsidiary
        if (currentSubsidiaryId === "6") {
          // RTS - existing positioning
          logoCSS = 'width:110px;height:110px;display:block;position:relative;left:-30px;';
        } else if (currentSubsidiaryId === "14" || currentSubsidiaryId === "15" || 
                   currentSubsidiaryId === "16" || currentSubsidiaryId === "9") {
          // C2O subsidiaries - move left over company name
          logoCSS = 'width:130px;height:55px;display:block;position:relative;left:-40px;';
        } else {
          // Default positioning
          logoCSS = 'width:130px;height:55px;display:block;';
        }

        // Create the new header HTML with memo on its own row, keeping original column structure
        var newHeaderHTML = 
'<table class="header" style="width: 100%;"><tr>' +
'<td style="width: 60%;" align="left"><span class="title">TAX INVOICE</span></td>' +
'<td align="right"><img src="' + logoURL + '" style="' + logoCSS + '" /></td></tr></table>' +
'<table class="header" style="width: 100%;"><tr>' +
'<td style="width: 60%; line-height: 125%;" align="left">' +
'<span style="font-size: 9pt;">' +
'<strong>ATTN:</strong><br />' +
customerName + '<br />' +
customerAddress + '<br/>' +
'ABN: ' + customerABN + '<br/>' +
'</span></td>' +
'<td style="width: 55%;" align="left" valign="top">' +
'<table><tr>' +
'<td style="font-size: 9pt; padding: 0 0 8px 0;"><span style="font-weight: bold;">Invoice Date:</span><br /><span style="font-weight: normal;">' + invoiceDate + '</span></td>' +
'</tr>' +
'<tr>' +
'<td style="font-size: 9pt; padding: 0 0 8px 0;"><span style="font-weight: bold;">Invoice Number:</span><br /><span style="font-weight: normal;">' + groupInvoiceNumber + '</span></td>' +
'</tr>' +
'<tr>' +
'<td style="font-size: 9pt; padding: 0 0 8px 0;"><span style="font-weight: bold;">PO Number:</span><br /><span style="font-weight: normal;">' + poNum + '</span></td>' +
'</tr>' +
'<tr>' +
'<td style="font-size: 9pt; padding: 0 0 8px 0;"><span style="font-weight: bold;">Customer Reference:</span><br /><span style="font-weight: normal;">' + customerRef + '</span></td>' +
'</tr></table>' +
'</td>' +
'<td style="width: 45%;" align="right" valign="top">' +
'<span style="font-size: 9pt; line-height: 125%;">' +
// Only show subsidiary info if it's different from customer name
(subsidiaryName.toLowerCase() !== customerName.toLowerCase() ? subsidiaryName + '<br />' : '') +
(subsidiaryRec.getValue('mainaddress_text') || '') + '<br /><br />' +
'<strong>ABN:</strong> ' + (subsidiaryRec.getValue('federalidnumber') || '') +
'</span></td>' +
'</tr></table>' +
// NEW: Put memo in its own separate table row with bottom margin and extra spacing
'<table class="header" style="width: 100%; margin-top: 10px; margin-bottom: 50px; padding-bottom: 30px;"><tr>' +
'<td colspan="3" style="width: 100%; line-height: 125%; padding-bottom: 30px;" align="left">' +
'<span style="font-size: 9pt;">' +
'<strong>Memo:</strong><br />' + memoField + '<br/><br/><br/>' +
'</span></td>' +
        '</tr></table>' +
        '<div style="height: 40px; clear: both;"></div>';

        var replaceLabor = isAustraliaSubsidiary;
        log.debug('replaceLabor', replaceLabor)
        
        if (replaceLabor) {
          summaryObj.forEach(function (entry) {
            for (var key in entry) {
              if (typeof entry[key] === 'string') {
                entry[key] = entry[key].replace(/\bLabor\b/g, 'Labour');
              }
            }
          });
        }

        // Create custom item table with GST Amount column - with large top spacing
        var itemTableHTML = 
'<p style="height: 80px; margin: 0; padding: 0;">&nbsp;</p>' +
'<p style="height: 80px; margin: 0; padding: 0;">&nbsp;</p>' +
'<table class="itemtable" style="width: 100%; border-collapse: collapse; margin-top: 80px !important; padding-top: 80px !important;">' +
'<thead>' +
'<tr>' +
'<th style="width: 40%; text-align: left; background-color: #657796; color: #ffffff; padding: 8px; font-weight: bold;">DESCRIPTION</th>' +
'<th style="width: 15%; text-align: center; background-color: #657796; color: #ffffff; padding: 8px; font-weight: bold;">PRICE</th>' +
'<th style="width: 15%; text-align: center; background-color: #657796; color: #ffffff; padding: 8px; font-weight: bold;">GST RATE</th>' +
'<th style="width: 15%; text-align: right; background-color: #657796; color: #ffffff; padding: 8px; font-weight: bold;">GST AMOUNT</th>' +
'<th style="width: 15%; text-align: right; background-color: #657796; color: #ffffff; padding: 8px; font-weight: bold;">AMOUNT AUD</th>' +
'</tr>' +
'</thead>' +
'<tbody>';

        // Add each summary item as a row
        for (var i = 0; i < summaryObj.length; i++) {
          var item = summaryObj[i];
          var rowColor = (i % 2 === 0) ? '#ffffff' : '#e1e6ee';
          
          log.debug('Item data for row ' + i, item); // Debug the item data
          
          itemTableHTML += 
'<tr style="background-color: ' + rowColor + ';">' +
'<td style="padding: 8px; border: 0.5px solid #657796;">' + item.category + '</td>' +
'<td style="padding: 8px; text-align: center; border: 0.5px solid #657796;">' + item.unitPrice + '</td>' +
'<td style="padding: 8px; text-align: center; border: 0.5px solid #657796;">' + (item.rate || '10.00%') + '</td>' +
'<td style="padding: 8px; text-align: right; border: 0.5px solid #657796;">' + item.gstAmount + '</td>' +
'<td style="padding: 8px; text-align: right; border: 0.5px solid #657796;">' + item.total + '</td>' +
'</tr>';
        }

        if (isAustraliaSubsidiary && totalRetention > 0) {
          for (var retentionIndex = 0; retentionIndex < retentionGroupOrder.length; retentionIndex++) {
            var retentionKey = retentionGroupOrder[retentionIndex];
            var retentionGroup = retentionGroups[retentionKey];
            var retentionRowColor = ((summaryObj.length + retentionIndex) % 2 === 0) ? '#ffffff' : '#e1e6ee';
            var retentionLabel = 'Retention';

            if (retentionGroup.percent) {
              retentionLabel += ' (less ' + retentionGroup.percent + ')';
            }

            itemTableHTML +=
'<tr style="background-color: ' + retentionRowColor + '; font-weight: bold;">' +
'<td style="padding: 8px; border: 0.5px solid #657796;">' + escapeXml(retentionLabel) + '</td>' +
'<td style="padding: 8px; text-align: center; border: 0.5px solid #657796;">' + formatCurrencyAccounting(retentionGroup.amount) + '</td>' +
'<td style="padding: 8px; text-align: center; border: 0.5px solid #657796;">&nbsp;</td>' +
'<td style="padding: 8px; text-align: right; border: 0.5px solid #657796;">&nbsp;</td>' +
'<td style="padding: 8px; text-align: right; border: 0.5px solid #657796;">&nbsp;</td>' +
'</tr>';
          }

          log.debug('Added retention summary rows', {
            totalRetention: totalRetention,
            retentionGroupCount: retentionGroupOrder.length
          });
        }

        itemTableHTML += '</tbody></table>';
        log.debug('Created custom item table with GST column');

        // Check if we need to add the disclaimer based on subsidiary ID
        var currentSubsidiaryId = subsidiaryRec.getValue('internalid') || subID.toString();
        var needsDisclaimer = (currentSubsidiaryId === "9" || currentSubsidiaryId === "6");
        log.debug('Disclaimer check', {subsidiaryId: currentSubsidiaryId, needsDisclaimer: needsDisclaimer});
        
        // TEMPORARY: Force disclaimer to show for testing
        needsDisclaimer = true;
        log.debug('Forcing disclaimer to show for testing');

        // Create disclaimer HTML if needed - REMOVED ASTERISKS
        var disclaimerHTML = '';
        if (needsDisclaimer) {
          disclaimerHTML = 
'<p style="font-size: 7pt; margin-top: 15px; padding: 10px;">' +
'We appreciate your business and prompt payment. All amounts payable under contracts to which this invoice relates, are to be transferred to Scottish Pacific Business Finance Pty Ltd (ScotPac). Payment to any other person will not constitute a valid discharge of the debt.' +
'</p>';
          log.debug('Created simplified disclaimer HTML');
        }

        // Debug: Check if itemtable exists in template
        log.debug('Looking for itemtable in template', xmlTemplateFile.indexOf('itemtable') > -1);
        log.debug('Template contains item?', xmlTemplateFile.indexOf('record.item') > -1);
        
        // SIMPLE TEXT REPLACEMENT TO CHANGE "Unit Price" to "Price"
        xmlTemplateFile = xmlTemplateFile.replace(/Unit Price/g, 'Price');
        xmlTemplateFile = xmlTemplateFile.replace(/UNIT PRICE/g, 'PRICE');
        xmlTemplateFile = xmlTemplateFile.replace(/unit price/g, 'price');
        log.debug('Changed Unit Price to Price in template');
        
        // REMOVE "PLEASE NOTE CHANGE IN" TEXT
        xmlTemplateFile = xmlTemplateFile.replace(/PLEASE NOTE CHANGE IN/g, '');
        xmlTemplateFile = xmlTemplateFile.replace(/Please note change in/g, '');
        xmlTemplateFile = xmlTemplateFile.replace(/please note change in/g, '');
        log.debug('Removed PLEASE NOTE CHANGE IN text');
        
        // Try multiple approaches to replace the item table
        var originalLength = xmlTemplateFile.length;
        
        // Add spacer before item table
        var spacedItemTableHTML = '<div style="height: 60px; clear: both;"></div>' + itemTableHTML;
        
        // Approach 1: Replace itemtable
        xmlTemplateFile = xmlTemplateFile.replace(/<table class="itemtable"[^>]*>.*?<\/table>/gs, spacedItemTableHTML);
        
        // Approach 2: Replace any table that contains item data
        if (xmlTemplateFile.length === originalLength) {
          xmlTemplateFile = xmlTemplateFile.replace(/<table[^>]*>[\s\S]*?result\.item[\s\S]*?<\/table>/g, spacedItemTableHTML);
        }
        
        // Approach 3: Replace FreeMarker item loop
        if (xmlTemplateFile.length === originalLength) {
          xmlTemplateFile = xmlTemplateFile.replace(/<#if record\.item\?has_content>[\s\S]*?<\/#if>/g, spacedItemTableHTML);
        }
        
        // Approach 4: Add after header if nothing was replaced - FIXED WITH MORE SPACING
        if (xmlTemplateFile.length === originalLength) {
          log.debug('No existing item table found, adding after header');
          xmlTemplateFile = xmlTemplateFile.replace(newHeaderHTML, newHeaderHTML + '<br/><br/><br/><br/>' + itemTableHTML + '<br/>');
        }
        
        log.debug('Item table replacement result', xmlTemplateFile.length !== originalLength ? 'SUCCESS' : 'FAILED');

        // Add the disclaimer in a safe location
        if (needsDisclaimer) {
          // Only try one safe insertion point - before closing body tag
          if (xmlTemplateFile.indexOf('</body>') > -1) {
            xmlTemplateFile = xmlTemplateFile.replace('</body>', disclaimerHTML + '</body>');
            log.debug('Added disclaimer before </body> tag');
          } else {
            // If no body tag, add after the item table
            xmlTemplateFile = xmlTemplateFile.replace(itemTableHTML, itemTableHTML + disclaimerHTML);
            log.debug('Added disclaimer after item table');
          }
        }

        // Add the required CSS styles with updated margin for itemtable
        var headerStyles = 
'<style type="text/css">' +
'span.title { font-size: 24pt; }' +
'table.header td { padding: 4px 6px; font-size: 10pt; }' +
'table.itemtable { width: 100%; border-collapse: collapse; margin: 30px 0 20px 0; }' +
'table.itemtable th { background-color: #657796; color: #ffffff; padding: 8px; font-weight: bold; font-size: 9pt; }' +
'table.itemtable td { padding: 8px; font-size: 9pt; border: 0.5px solid #657796; }' +
'</style>';

        // Inject the styles into the template
        xmlTemplateFile = xmlTemplateFile.replace('</head>', headerStyles + '</head>');
        log.debug('Added header styles');

        // Instead of removing content, let's replace the header macro content
        // Look for the nlheader macro and replace its content
        var headerMacroRegex = /(<macro id="nlheader">)([\s\S]*?)(<\/macro>)/;
        if (xmlTemplateFile.match(headerMacroRegex)) {
          xmlTemplateFile = xmlTemplateFile.replace(headerMacroRegex, '$1' + newHeaderHTML + '$3');
          log.debug('Replaced header macro content');
        } else {
          // If no header macro, just add after body
          xmlTemplateFile = xmlTemplateFile.replace(/(<body[^>]*>)/, '$1' + newHeaderHTML);
          log.debug('Added header after body tag');
        }

        // Add GST column info to template replacements
        xmlTemplateFile = xmlTemplateFile.replace('${itemtotal}', "$" + formatCurrency(subtotal));
        var taxtotal = totalTax;
        xmlTemplateFile = xmlTemplateFile.replace('${taxtotal}', "$" + formatCurrency(taxtotal));
        xmlTemplateFile = xmlTemplateFile.replace('${total}', "$" + formatCurrency(totalMain));
        xmlTemplateFile = xmlTemplateFile.replace('${ponum}', poNum);
        xmlTemplateFile = xmlTemplateFile.replace('${customerref}', customerRef);
        xmlTemplateFile = xmlTemplateFile.replace('${projectMan}', projectMan);
        xmlTemplateFile = xmlTemplateFile.replace('${logoURL}', logoURL);
        
        // Add GST column header if template has column structure
        xmlTemplateFile = xmlTemplateFile.replace('${item.amount@label?upper_case}', 'GST AMOUNT</th><th colspan="3" align="center">AMOUNT');
        
        // Try to add GST data to any existing item loops
        xmlTemplateFile = xmlTemplateFile.replace('${item.taxamount}', '${item.gstAmount}');
        
        log.debug('Applied template replacements including GST column modifications');

        log.debug('About to set template content and add data sources');
        renderer.templateContent = xmlTemplateFile;

        // Add our custom item data with GST amounts - fix the data structure
        renderer.addCustomDataSource({
          format: render.DataSource.OBJECT,
          alias: 'customItems',
          data: {items: summaryObj}
        });

        // Keep the original item data source format
        renderer.addCustomDataSource({
          format: render.DataSource.OBJECT,
          alias: 'item',
          data: {result: summaryObj}
        });

        renderer.addRecord('record', invoiceGroupRec);
        renderer.addRecord('subsidiary', subsidiaryRec);

        log.debug('About to render PDF');
        var pdfFile = renderer.renderAsPdf();
        
        pdfFile.name = 'Invoice_Group_Summary_' + recID + '.pdf';
        
        log.debug('About to write file');
        context.response.writeFile(pdfFile, true);
        log.debug('File written successfully');
        
      } catch (e) {
        log.error('Main error', e);
        // Return a simple error page instead of crashing
        context.response.write('Error generating PDF: ' + e.message);
      }
    }
  }

  function formatCurrency(value) {
    const number = parseAmount(value);
    const sign = number < 0 ? '-' : '';
    const currencyString = Math.abs(number).toFixed(2);
    const [integerPart, decimalPart] = currencyString.split('.');
    const withCommas = integerPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    return sign + withCommas + '.' + decimalPart;
  }

  function formatCurrencyAccounting(value) {
    return '($' + formatCurrency(Math.abs(parseAmount(value))) + ')';
  }

  function addRetentionGroup(retentionGroups, retentionGroupOrder, percent, amount) {
    var key = percent || '__blank__';

    if (!retentionGroups[key]) {
      retentionGroups[key] = {
        percent: percent,
        amount: 0
      };
      retentionGroupOrder.push(key);
    }

    retentionGroups[key].amount += amount;
  }

  function parseAmount(value) {
    if (value === null || value === undefined || value === '') return 0;

    var cleanValue = value.toString().trim();
    var isAccountingNegative = cleanValue.charAt(0) === '(' && cleanValue.charAt(cleanValue.length - 1) === ')';

    cleanValue = cleanValue.replace(/[,$%\s]/g, '').replace(/[()]/g, '');

    var number = parseFloat(cleanValue);
    if (isNaN(number)) return 0;

    return isAccountingNegative ? -number : number;
  }

  function formatPercent(value) {
    var number = parsePercentValue(value);
    if (!number) return '';

    return (number % 1 === 0 ? number.toFixed(0) : number.toFixed(2).replace(/0+$/, '').replace(/\.$/, '')) + '%';
  }

  function parsePercentValue(value) {
    if (value === null || value === undefined || value === '') return 0;

    var stringValue = value.toString();
    var number = parseAmount(value);

    if (number > 0 && number <= 1 && stringValue.indexOf('%') === -1) {
      number = number * 100;
    }

    return number;
  }

  function escapeXml(value) {
    if (value === null || value === undefined) return '';

    return value.toString()
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&apos;');
  }

  return { onRequest };
});
