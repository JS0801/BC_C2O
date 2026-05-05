/**
* @NApiVersion 2.1
* @NScriptType UserEventScript
*/
define(['N/record', 'N/search', 'N/log'], function(record, search, log) {
  function afterSubmit(context) {
    if (context.type !== context.UserEventType.CREATE && context.type !== context.UserEventType.EDIT) return; // Run only on Create
    
    try {
      var newRecord = context.newRecord;
      var salesOrderId = newRecord.id; // Get newly created Sales Order ID
      
      if (!salesOrderId) {
        log.debug('No Sales Order ID found. Exiting script.');
        return;
      }
      log.debug('Processing Sales Order:', salesOrderId);
      
      var projectFields = search.lookupFields({
        type: 'customrecord_cseg_bc_project',
        id: newRecord.getValue('cseg_bc_project'),
        columns: ['custrecord_bc_proj_tm_billing_template']
      });
      
      var billingTemplateId = null;
      var textMap = {};
      
      if (
        projectFields.custrecord_bc_proj_tm_billing_template &&
        projectFields.custrecord_bc_proj_tm_billing_template.length
      ) {
        billingTemplateId = projectFields.custrecord_bc_proj_tm_billing_template[0].value;
      }
      if (!billingTemplateId) return;
      
      
      var customrecord_bc_tm_billing_labor_detailSearchObj = search.create({
        type: "customrecord_bc_tm_billing_labor_detail",
        filters:
        [
          ["custrecord_bc_tm_billing_template","anyof", billingTemplateId],
          "AND",
          ["custrecord_bc_tm_billing_detail_b_class","noneof", "@NONE@"]
        ],
        columns:
        [  
          search.createColumn({name: "internalid", join: "CUSTRECORD_BC_TM_BILLING_DETAIL_B_CLASS", summary: "GROUP" }),
          search.createColumn({name: "custrecord_bc_tm_billing_detail_b_class", summary: "MAX", label: "T&M Billing Class"}),
          search.createColumn({name: "custrecord_show_on_pdf_as", summary: "MAX", label: "Show on PDF as"})
        ]
      });
      var searchResultCount = customrecord_bc_tm_billing_labor_detailSearchObj.runPaged().count;
      log.debug("customrecord_bc_tm_billing_labor_detailSearchObj result count",searchResultCount);
      customrecord_bc_tm_billing_labor_detailSearchObj.run().each(function(result){
        log.debug('result', result)
        textMap[result.getValue({name: "custrecord_bc_tm_billing_detail_b_class", summary: "MAX"})] = result.getValue({name: "custrecord_show_on_pdf_as", summary: "MAX"}) || result.getValue({name: "custrecord_bc_tm_billing_detail_b_class", summary: "MAX"});
        return true;
      });

      log.debug('textMap', textMap)
      
      var salesOrderSearchObj = search.create({
        type: "salesorder",
        filters: [
          ["internalid", "anyof", salesOrderId],
          "AND",
          ["custcol_bc_tm_time_bill", "noneof", "@NONE@"],
          "AND",
          ["custcol_c2o_billing_class_override", "isempty", ""]
        ],
        columns: [
          search.createColumn({ name: "line", label: "Line ID" }),
          search.createColumn({ name: "custcol_bc_tm_labor_billing_class", join: "CUSTCOL_BC_TM_TIME_BILL"})
        ]
      });
      
      var searchResults = salesOrderSearchObj.run().getRange({ start: 0, end: 1000 });
      
      if (searchResults.length === 0) {
        log.debug("No matching lines found for Sales Order ID: " + salesOrderId);
        return;
      }
      
      var salesOrderRec = record.load({
        type: record.Type.SALES_ORDER,
        id: salesOrderId
      });
      
      searchResults.forEach(function(result) {
        var lineNum = result.getValue({ name: "line" });
        //log.debug('result', result)
        
        if (!lineNum) return;
        
        var lineIndex = salesOrderRec.findSublistLineWithValue({
          sublistId: 'item',
          fieldId: 'line',
          value: lineNum
        });
        
        if (lineIndex === -1) return;
        
        var columns = result.columns;
        columns.forEach(function(col) {
          var fieldId = col.name;
          var joinId = col.join;
          var value = result.getText({ name: fieldId, join: joinId });
          
          if (value !== null && fieldId !== 'line') {
            salesOrderRec.setSublistValue({
              sublistId: 'item',
              fieldId: 'custcol_c2o_billing_class_override',
              line: lineIndex,
              value: textMap[value] || ''
            });
            // log.debug(`Set field ${fieldId} on line ${lineIndex}`, value);
          }
        });
      });
      
      salesOrderRec.save();
      log.debug("Sales Order Updated Successfully", salesOrderId);
      
    } catch (error) {
      log.error("Error in afterSubmit script", error);
    }
  }
  
  return {
    afterSubmit: afterSubmit
  };
});
