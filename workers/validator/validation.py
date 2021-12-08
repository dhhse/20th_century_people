import csv

from .validation_logging import EntryLogJournal, MissingFieldEntry, ExcessFieldEntry

class RecordValidator:
    def __init__(self, config):
        self.config = config

    def validate(self, record):
        validation_log = EntryLogJournal()

        # check necessary fields
        for field_name, kind in self.config.items():
            if field_name not in record:
                if (kind == 'service') or (kind == 'required'):
                    level = 'error'
                elif (kind == 'recommended'):
                    level = 'warning'
                else:
                    level = None

                if level:
                    validation_log.append( MissingFieldEntry(level, field_name, kind) )

        # check extra fields
        for field_name, value in record.items():
            if field_name not in self.config:
                validation_log.append( ExcessFieldEntry("error", field_name) )

        return validation_log

def read_validation_config(field_descriptions_fn):
    validation_config = {}
    with open(field_descriptions_fn) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for line in reader:
            validation_config[line['field_name']] = line['kind']
    return validation_config
