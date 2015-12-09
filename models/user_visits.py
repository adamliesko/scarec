import redis_client


class UserVisits:

    def __init__(self, user):
        self.user = user
        self.client = redis_client.get_client()

    def user_visited_items(self):
        self.client.get('user_visits:' + self.user)
        pass

    def user_visited_items_intersection(self, item_ids):
        pass

    def user_has_not_visited_items(self, item_ids):
        pass

    def add_user_item_visit(self, item_id, timestamp):
        pass

    def user_most_recent_visited_items(self, item_count):
        pass




