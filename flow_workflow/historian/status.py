VALID_STATUSES = [
        'unknown',
        'new',
        'running*',#for shortcutting
        'scheduled',
        'running',
        'crashed',
        'done',
]

RUNNING_STATUSES = [
        'running',
        'scheduled',
        ]

DONE_STATUSES = [
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

    @property
    def is_running(self):
        return self.text in RUNNING_STATUSES

    @property
    def is_done(self):
        return self.text in DONE_STATUSES

    def __str__(self):
        return str(self.text)

    def __repr__(self):
        return "Status(text='%s')" % self.text

