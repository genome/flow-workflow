VALID_STATUSES = [
        'unknown',
        'new',
        'scheduled',
        'running',
        'failed',
        'crashed',
        'done',
]

class Status(object):
    def __init__(self, text=VALID_STATUSES[0]):
        self.text = text
        self.validate()

    def validate(self):
        if self.text not in VALID_STATUSES:
            raise ValueError("Status must be one of %s not %s." %
                    (VALID_STATUSES, self.text))

    def should_overwrite(self, other):
        return other < self

    @property
    def index(self):
        return VALID_STATUSES.index(self.text)

    def __cmp__(self, other):
        return cmp(self.index, other.index)

