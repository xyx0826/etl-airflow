import re

def flatten_salesforce_record(rec):
    new_rec = {}
    for k,v in rec.items():
        # if it's a relationship...
        if k.endswith('__r'):
            try:
                # iterate through the OrderedDict we're given
                for a, b in v.items():
                    # ignore these keys
                    if a not in ['type', 'url', 'attributes']:
                        # construct a new key from the top-level key and the field name
                        key = re.sub("__r$", "", k) + '_' + re.sub("__c$", "", a)
                        # create it
                        new_rec[key] = b
            except AttributeError:
                pass
        # strip off '__c'
        elif k.endswith('__c'):
            new_rec[re.sub("__c$", "", k)] = v
        # no __c? just add it
        else:
            new_rec[k] = v
    return new_rec
