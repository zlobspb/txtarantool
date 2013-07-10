host = "localhost"
port = 33013

sock = "/tmp/tarantool.sock"

charset = "utf-8"
errors = "strict"

# see tarantool.cfg
space_no0 = 0
space_no1 = 1

import string
insert_string_choice = string.ascii_letters + string.digits
# randint(0, insert_string_length_max)
insert_string_length_max = 256
