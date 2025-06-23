from enum import Enum, auto


class BucketMode(Enum):

    def __str__(self):
        return self.value

    HASH_FIXED = auto()

    HASH_DYNAMIC = auto()

    CROSS_PARTITION = auto()

    BUCKET_UNAWARE = auto()
