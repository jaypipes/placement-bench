VCPU = 1
RAM_MB = 2
DISK_GB = 3

RESOURCE_TEMPLATES = (
    {
        # "micro"
        VCPU: 1,
        RAM_MB: 64,
    },
    {
        # "small"
        VCPU: 1,
        RAM_MB: 1024,
    },
    {
        # "medium"
        VCPU: 2,
        RAM_MB: 4096,
    },
    {
        # "large"
        VCPU: 4,
        RAM_MB: 8192,
    },
    {
        # "xlarge"
        VCPU: 8,
        RAM_MB: 16384,
    },
    {
        # "xxlarge"
        VCPU: 16,
        RAM_MB: 32768,
    },
)
