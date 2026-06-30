/**
 * @NApiVersion 2.1
 * @NScriptType WorkflowActionScript
 */
define(['N/search', 'N/runtime', 'N/log'], (search, runtime, log) => {
  const ACTION_PARAM = 'custscript_bc_pobill_action'; // INIT or APPROVE

  const RECORDS = {
    APPROVAL_ROUTING: 'customrecord_c2o_approval_routing',
    SUBSIDIARY: 'subsidiary'
  };

  const FIELDS = {
    NEXT_APPROVER: 'nextapprover',
    SUBSIDIARY: 'subsidiary',
    DEPARTMENT: 'department',
    VENDOR: 'entity',
    TOTAL: 'total',

    TXN_ROUTING_RULE: 'custbody_c2o_pobill_approval_routing',
    TXN_SEQUENCE: 'custbody_current_app_sequence',
    TXN_STATUS: 'custbody_c2o_pobill_routing_status',
    TXN_ERROR: 'custbody_c2o_pobill_routing_error',

    // Change these if the account uses different transaction/subsidiary fields.
    SUBSIDIARY_REGION: 'custrecord_c2o_region',
    PROJECT_FLAG: 'custbody_bc_project',
    BILLABLE_FLAG: 'custbody_billable',

    RULE_REGION: 'custrecord_approval_region',
    RULE_DEPARTMENT: 'custrecord_approval_department',
    RULE_MIN: 'custrecord_approval_min_threshold',
    RULE_MAX: 'custrecord_approval_max_threshold',
    RULE_APPROVER: 'custrecord_approval_approver',
    RULE_TYPE: 'custrecord_approval_type',
    RULE_PROJECT: 'custrecord_approval_project',
    RULE_BILLABLE: 'custrecord_approval_billable',
    RULE_VENDOR: 'custrecord_approval_vendor',
    RULE_SEQUENCE: 'custrecord_approval_sequence',
    RULE_BACKUP_APPROVER: 'custrecord_backup_approver',
    RULE_ACCOUNT: 'custrecord_approval_account'
  };

  const STATUS = {
    PENDING_APPROVAL: 1,
    APPROVED: 2,
    REJECTED: 3,
    NO_RULE_FOUND: 4,
    ERROR: 5
  };

  function onAction(context) {
    const rec = context.newRecord;
    const action = getAction();

    try {
      const txn = getTransactionValues(rec);
      log.audit('PO/Bill routing started', {
        action,
        recordType: rec.type,
        recordId: rec.id || '',
        amount: txn.amount,
        ruleType: txn.ruleTypeText,
        department: txn.department,
        region: txn.region,
        vendor: txn.vendor,
        accounts: txn.accounts,
        project: txn.project,
        billable: txn.billable
      });

      if (action === 'APPROVE') {
        return approveStep(rec, txn);
      }

      return initRoute(rec, txn);
    } catch (e) {
      log.error('PO/Bill routing error', {
        message: e.message,
        stack: e.stack || ''
      });
      safeSet(rec, FIELDS.TXN_STATUS, STATUS.ERROR);
      safeSet(rec, FIELDS.TXN_ERROR, e.message || String(e));
      return 'ERROR';
    }
  }

  function initRoute(rec, txn) {
    if (hasExistingPendingRoute(rec)) {
      log.audit('Existing pending PO/Bill route kept', {
        routingRule: value(rec, FIELDS.TXN_ROUTING_RULE),
        sequence: value(rec, FIELDS.TXN_SEQUENCE),
        nextApprover: value(rec, FIELDS.NEXT_APPROVER)
      });
      return 'PENDING_APPROVAL';
    }

    const rules = findMatchingRules(txn);
    const selected = rules[0];

    if (!selected) {
      clearApproval(rec);
      safeSet(rec, FIELDS.TXN_STATUS, STATUS.NO_RULE_FOUND);
      safeSet(rec, FIELDS.TXN_ERROR, buildNoRuleMessage(txn));
      log.error('No PO/Bill approval rule found', txn);
      return 'NO_RULE_FOUND';
    }

    applyRule(rec, selected);
    safeSet(rec, FIELDS.TXN_STATUS, STATUS.PENDING_APPROVAL);
    safeSet(rec, FIELDS.TXN_ERROR, '');

    log.audit('Initial PO/Bill approver selected', selected);
    return 'PENDING_APPROVAL';
  }

  function hasExistingPendingRoute(rec) {
    return String(value(rec, FIELDS.TXN_STATUS)) === String(STATUS.PENDING_APPROVAL)
      && Boolean(value(rec, FIELDS.TXN_ROUTING_RULE))
      && Boolean(value(rec, FIELDS.NEXT_APPROVER));
  }

  function approveStep(rec, txn) {
    const currentRuleId = value(rec, FIELDS.TXN_ROUTING_RULE);

    if (!currentRuleId) {
      log.audit('Approve step has no current rule; rerouting from start', {});
      return initRoute(rec, txn);
    }

    const currentRule = loadRule(currentRuleId);
    const currentSequence = number(currentRule.sequence);
    const candidates = findMatchingRules(txn);
    log.debug('Next approval candidates before route-group filter', {
      currentRule,
      currentSequence,
      candidates: candidates.map((rule) => ({
        id: rule.id,
        name: rule.name,
        sequence: rule.sequence,
        min: rule.min,
        max: rule.max,
        region: rule.region,
        department: rule.department,
        vendor: rule.vendor,
        account: rule.account,
        project: rule.project,
        billable: rule.billable
      }))
    });

    const nextRule = candidates
      .filter((rule) => sameRouteGroup(rule, currentRule))
      .filter((rule) => number(rule.sequence) > currentSequence)
      .sort(sortRules)[0];
    log.debug('Next approval rule selected', nextRule || null);

    if (nextRule) {
      applyRule(rec, nextRule);
      safeSet(rec, FIELDS.TXN_STATUS, STATUS.PENDING_APPROVAL);
      safeSet(rec, FIELDS.TXN_ERROR, '');
      log.audit('Next PO/Bill approver selected', {
        currentRuleId,
        currentSequence,
        nextRule
      });
      return 'PENDING_APPROVAL';
    }

    safeSet(rec, FIELDS.NEXT_APPROVER, '');
    safeSet(rec, FIELDS.TXN_STATUS, STATUS.APPROVED);
    safeSet(rec, FIELDS.TXN_ERROR, '');

    log.audit('PO/Bill approval route complete', {
      currentRuleId,
      currentSequence
    });
    return 'APPROVED';
  }

  function applyRule(rec, rule) {
    safeSet(rec, FIELDS.NEXT_APPROVER, rule.approver);
    safeSet(rec, FIELDS.TXN_ROUTING_RULE, rule.id);
    safeSet(rec, FIELDS.TXN_SEQUENCE, rule.sequence || 1);
  }

  function clearApproval(rec) {
    safeSet(rec, FIELDS.NEXT_APPROVER, '');
    safeSet(rec, FIELDS.TXN_ROUTING_RULE, '');
    safeSet(rec, FIELDS.TXN_SEQUENCE, '');
  }

  function findMatchingRules(txn) {
    const columns = [
      'name',
      FIELDS.RULE_TYPE,
      FIELDS.RULE_MIN,
      FIELDS.RULE_MAX,
      FIELDS.RULE_REGION,
      FIELDS.RULE_DEPARTMENT,
      FIELDS.RULE_APPROVER,
      FIELDS.RULE_PROJECT,
      FIELDS.RULE_BILLABLE,
      FIELDS.RULE_VENDOR,
      FIELDS.RULE_ACCOUNT,
      FIELDS.RULE_SEQUENCE,
      FIELDS.RULE_BACKUP_APPROVER
    ];

    const results = [];

    search.create({
      type: RECORDS.APPROVAL_ROUTING,
      filters: [
        ['isinactive', 'is', 'F'],
        'AND',
        [FIELDS.RULE_MIN, 'onorbefore', txn.amount],
        'AND',
        [FIELDS.RULE_MAX, 'onorafter', txn.amount]
      ],
      columns
    }).run().each((result) => {
      const rule = readRuleResult(result);

      if (ruleMatches(rule, txn)) {
        results.push(rule);
      }

      return true;
    });

    results.sort(sortRules);
    log.debug('Matching PO/Bill approval rules', {
      count: results.length,
      firstRule: results[0] || null
    });

    return results;
  }

  function readRuleResult(result) {
    return {
      id: result.id,
      name: result.getValue({ name: 'name' }) || '',
      typeText: result.getText({ name: FIELDS.RULE_TYPE }) || '',
      min: result.getValue({ name: FIELDS.RULE_MIN }),
      max: result.getValue({ name: FIELDS.RULE_MAX }),
      region: result.getValue({ name: FIELDS.RULE_REGION }) || '',
      department: result.getValue({ name: FIELDS.RULE_DEPARTMENT }) || '',
      approver: result.getValue({ name: FIELDS.RULE_APPROVER }) || '',
      project: boolValue(result.getValue({ name: FIELDS.RULE_PROJECT })),
      billable: boolValue(result.getValue({ name: FIELDS.RULE_BILLABLE })),
      vendor: result.getValue({ name: FIELDS.RULE_VENDOR }) || '',
      account: result.getValue({ name: FIELDS.RULE_ACCOUNT }) || '',
      sequence: result.getValue({ name: FIELDS.RULE_SEQUENCE }) || '',
      backupApprover: result.getValue({ name: FIELDS.RULE_BACKUP_APPROVER }) || ''
    };
  }

  function loadRule(ruleId) {
    const lookup = search.lookupFields({
      type: RECORDS.APPROVAL_ROUTING,
      id: ruleId,
      columns: [
        'name',
        FIELDS.RULE_TYPE,
        FIELDS.RULE_MIN,
        FIELDS.RULE_MAX,
        FIELDS.RULE_REGION,
        FIELDS.RULE_DEPARTMENT,
        FIELDS.RULE_APPROVER,
        FIELDS.RULE_PROJECT,
        FIELDS.RULE_BILLABLE,
        FIELDS.RULE_VENDOR,
        FIELDS.RULE_ACCOUNT,
        FIELDS.RULE_SEQUENCE,
        FIELDS.RULE_BACKUP_APPROVER
      ]
    });

    return {
      id: String(ruleId),
      name: lookup.name || '',
      typeText: textValue(lookup[FIELDS.RULE_TYPE]),
      min: lookup[FIELDS.RULE_MIN],
      max: lookup[FIELDS.RULE_MAX],
      region: idValue(lookup[FIELDS.RULE_REGION]),
      department: idValue(lookup[FIELDS.RULE_DEPARTMENT]),
      approver: idValue(lookup[FIELDS.RULE_APPROVER]),
      project: boolValue(lookup[FIELDS.RULE_PROJECT]),
      billable: boolValue(lookup[FIELDS.RULE_BILLABLE]),
      vendor: idValue(lookup[FIELDS.RULE_VENDOR]),
      account: idValue(lookup[FIELDS.RULE_ACCOUNT]),
      sequence: lookup[FIELDS.RULE_SEQUENCE] || '',
      backupApprover: idValue(lookup[FIELDS.RULE_BACKUP_APPROVER])
    };
  }

  function ruleMatches(rule, txn) {
    if (!typeMatches(rule.typeText, txn.ruleTypeText)) return false;
    if (!rule.approver) return false;
    if (rule.region && String(rule.region) !== String(txn.region)) return false;
    if (rule.department && String(rule.department) !== String(txn.department)) return false;
    if (rule.vendor && String(rule.vendor) !== String(txn.vendor)) return false;
    if (rule.account && txn.accounts.indexOf(String(rule.account)) === -1) return false;
    if (rule.project && !txn.project) return false;
    if (rule.billable && !txn.billable) return false;
    return true;
  }

  function sameRouteGroup(a, b) {
    return number(a.min) === number(b.min)
      && number(a.max) === number(b.max)
      && id(a.region) === id(b.region)
      && id(a.department) === id(b.department)
      && id(a.vendor) === id(b.vendor)
      && id(a.account) === id(b.account)
      && Boolean(a.project) === Boolean(b.project)
      && Boolean(a.billable) === Boolean(b.billable)
      && typeMatches(a.typeText, b.typeText);
  }

  function sortRules(a, b) {
    const specificA = specificity(a);
    const specificB = specificity(b);
    if (specificA !== specificB) return specificB - specificA;

    const seqA = number(a.sequence) || 999999;
    const seqB = number(b.sequence) || 999999;
    if (seqA !== seqB) return seqA - seqB;

    return number(a.id) - number(b.id);
  }

  function specificity(rule) {
    let score = 0;
    if (rule.vendor) score += 100;
    if (rule.account) score += 100;
    if (rule.department) score += 10;
    if (rule.region) score += 10;
    if (rule.project) score += 1;
    if (rule.billable) score += 1;
    return score;
  }

  function getTransactionValues(rec) {
    const subsidiary = value(rec, FIELDS.SUBSIDIARY);
    const region = subsidiary ? lookupSubsidiaryRegion(subsidiary) : '';
    const amount = number(value(rec, FIELDS.TOTAL));

    return {
      ruleTypeText: getRuleTypeText(rec.type),
      amount,
      department: value(rec, FIELDS.DEPARTMENT),
      vendor: value(rec, FIELDS.VENDOR),
      subsidiary,
      region,
      project: Boolean(value(rec, FIELDS.PROJECT_FLAG)),
      billable: Boolean(value(rec, FIELDS.BILLABLE_FLAG)),
      accounts: getTransactionAccounts(rec)
    };
  }

  function lookupSubsidiaryRegion(subsidiaryId) {
    const lookup = search.lookupFields({
      type: RECORDS.SUBSIDIARY,
      id: subsidiaryId,
      columns: [FIELDS.SUBSIDIARY_REGION]
    });
    return idValue(lookup[FIELDS.SUBSIDIARY_REGION]);
  }

  function getTransactionAccounts(rec) {
    const accountIds = {};
    ['expense', 'item'].forEach((sublistId) => {
      const lineCount = safeLineCount(rec, sublistId);
      for (let i = 0; i < lineCount; i += 1) {
        const account = safeLineValue(rec, sublistId, 'account', i);
        if (account) accountIds[String(account)] = true;
      }
    });
    return Object.keys(accountIds);
  }

  function getRuleTypeText(recordType) {
    const type = String(recordType || '').toLowerCase();
    if (type === 'purchaseorder') return 'PO';
    if (type === 'vendorbill') return 'Bill';
    throw new Error(`Unsupported transaction type: ${recordType}`);
  }

  function typeMatches(ruleTypeText, targetTypeText) {
    const rule = String(ruleTypeText || '').toLowerCase();
    const target = String(targetTypeText || '').toLowerCase();

    if (target === 'po') {
      return rule === 'po' || rule.indexOf('purchase') !== -1;
    }

    if (target === 'bill') {
      return rule === 'bill' || rule.indexOf('vendor bill') !== -1;
    }

    return rule === target;
  }

  function getAction() {
    const action = runtime.getCurrentScript().getParameter({ name: ACTION_PARAM }) || 'INIT';
    return String(action).toUpperCase();
  }

  function buildNoRuleMessage(txn) {
    return `No approval rule found. Type=${txn.ruleTypeText}, Amount=${txn.amount}, Department=${txn.department || ''}, Region=${txn.region || ''}, Vendor=${txn.vendor || ''}`;
  }

  function value(rec, fieldId) {
    try {
      return rec.getValue({ fieldId }) || '';
    } catch (e) {
      log.debug('Field not available', { fieldId, message: e.message });
      return '';
    }
  }

  function safeSet(rec, fieldId, val) {
    try {
      rec.setValue({ fieldId, value: val });
    } catch (e) {
      log.debug('Could not set field', { fieldId, value: val, message: e.message });
    }
  }

  function safeLineCount(rec, sublistId) {
    try {
      return rec.getLineCount({ sublistId }) || 0;
    } catch (e) {
      return 0;
    }
  }

  function safeLineValue(rec, sublistId, fieldId, line) {
    try {
      return rec.getSublistValue({ sublistId, fieldId, line }) || '';
    } catch (e) {
      return '';
    }
  }

  function idValue(value) {
    if (Array.isArray(value) && value.length) return String(value[0].value || '');
    return value ? String(value) : '';
  }

  function id(value) {
    return String(value || '').trim();
  }

  function textValue(value) {
    if (Array.isArray(value) && value.length) return String(value[0].text || '');
    return value ? String(value) : '';
  }

  function number(value) {
    const parsed = parseFloat(value || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function boolValue(value) {
    return value === true || value === 'T' || value === 'true';
  }

  return { onAction };
});
