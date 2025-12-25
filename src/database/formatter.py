def format_bytes(bytes_size):
    bytes_size = float(bytes_size)
    if bytes_size == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0 or unit == 'GB':
            break
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} {unit}"


def format_number(num):
    num = int(num)
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return str(num)


def format_time_ns(elapsed_ns):
    elapsed_ns = int(elapsed_ns)
    """Format nanoseconds to human readable time"""
    if elapsed_ns < 1000:  # Less than 1 microsecond
        return f"{elapsed_ns:.0f}ns"
    elif elapsed_ns < 1_000_000:  # Less than 1 millisecond
        return f"{elapsed_ns/1000:.0f}µs"
    elif elapsed_ns < 1_000_000_000:  # Less than 1 second
        return f"{elapsed_ns/1_000_000:.0f}ms"
    else:  # 1 second or more
        return f"{elapsed_ns/1_000_000_000:.3f}s"
