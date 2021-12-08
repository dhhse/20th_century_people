import dataclasses

@dataclasses.dataclass(frozen=True)
class MissingFieldEntry:
    level: str
    field_name: str
    kind: str

    def __str__(self):
        return f"({self.level}) Field {self.field_name} ({self.kind}) is missing"

@dataclasses.dataclass(frozen=True)
class ExcessFieldEntry:
    level: str
    field_name: str

    def __str__(self):
        return f"({self.level}) Excess field {self.field_name}"

class EntryLogJournal:
    def __init__(self, entries = None):
        if entries is None:
            entries = []
        self.entries = entries

    def __len__(self):
        return len(self.entries)

    def __iter__(self):
        return iter(self.entries)

    def is_empty(self):
        return len(self.entries) == 0

    def is_not_empty(self):
        return len(self.entries) > 0

    def append(self, entry):
        self.entries.append(entry)

    def filtered_by_level(self, level):
        filtered_entries = [msg for msg in self.entries  if msg.level == level]
        return EntryLogJournal(filtered_entries)

    def is_valid(self):
        return len(self.filtered_by_level("error").entries) == 0
    
    def __str__(self):
        return "\n".join(str(msg) for msg in self.entries)
