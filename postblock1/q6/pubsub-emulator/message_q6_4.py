from hashlib import blake2b, blake2s
import random
from faker import Faker
import datetime

# sneaky globals
h = blake2b(key=b'pseudorandom key', digest_size=4)
# sneaky configs
random.seed(1002)  # do not change!

Faker.seed(1002)  # do not change!
fake = Faker(['en_US'])
fake.seed_instance(1002)  # do not change!


##
# For the sake of the assignment, you MUST use
# NUM_USERS = int(1e3)
# NUM_RECORDS = int(1e5)
# but feel free to change these for experimentation (fewer == easier to understand)
##
NUM_USERS = int(1e3)
NUM_RECORDS = int(1e5)



def record(user_id, user_name, url):
    """
    Generate a record with possible delays to simulate late data.
    """
    now = datetime.datetime.utcnow()
    delay_seconds = random.choice([0, 6, 12]) if random.random() < 0.1 else 0 
    event_time = now - datetime.timedelta(seconds=delay_seconds)
    return [user_id, user_name, url, event_time.strftime("%Y-%m-%d %H:%M:%S"), random.randint(0, 512)]



def users(nr_of):
    """
    produce a (username, userid) tuple that is unique in the returned list
    :param nr_of: the number of users to generate
    :return: a list of tuples
    """
    name_bag = set()
    for x in range(0, nr_of):
        name = fake.name()
        h.update(name.encode("utf-8"))
        name_bag.add((h.hexdigest(), name))
    return sorted(list(name_bag))


def record_generator(name_bag, min_records=10):
    for x in range(0, min_records):
        user_id, user_name = random.choice(name_bag)
        # Allow a user to get stuck on a url
        url = fake.url()
        yield record(user_id, user_name, url)
        while random.randint(0, 10) <= 3:
            yield record(user_id, user_name, url)



