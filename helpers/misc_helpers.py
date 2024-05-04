from datetime import datetime


def get_now_timestamp():
    return datetime.today().strftime("%Y-%m-%d %H:%M:%S")


def get_now_filename():
    return datetime.today().strftime("%Y%m%d%H%M%S")


if __name__ == "__main__":
    print(get_now_timestamp())
    print(get_now_filename())
