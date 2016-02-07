class Context:
    USABLE_PROPERTIES = []
    MAPPINGS = {
        '57': 'user_id',
        '27': 'publisher_id',
        '25': 'item_id'
    }

    MAPPINGS_INV = {
        'user_id': '57',
        'item_id': '25',
        'publisher_id': '27',
    }

    def __init__(self, dict_in):
        self.dict_in = dict_in
        self.dict_out = {}

    def _extract_clustered_content(self, parent_key, dict_in):
        for key, value in dict_in.items():
            new_key = parent_key + ':' + key
            if isinstance(value, int):
                self.dict_out[new_key] = value
            elif isinstance(value, list):
                self.dict_out = self._extract_list_with_seq_keys(new_key, value)
                pass
        return self.dict_out

    def _extract_list_with_seq_keys(self, key, list_in):
        idx = 0
        for value in list_in:
            self.dict_out[key + ':' + str(idx)] = value
            idx += 1
        return self.dict_out

    def extract_to_json(self):
        self.dict_out["recs"] = self.dict_in["recs"]["ints"]["3"]
        self.dict_out["recs"] = self.dict_in["timestamp"]
        for key, value in self.dict_in["context"]["simple"].items():
            self.dict_out[key] = value
        for key, value in self.dict_in['context']['lists'].items():
            self.dict_out[key] = value
        for key, value in self.dict_in['context']['clusters'].items():
            if isinstance(value, dict):
                self.dict_out = self._extract_clustered_content(key, value)
            elif isinstance(value, list):
                self.dict_out = self._extract_list_with_seq_keys(key, value)
        self.dict_out['timestamp'] = self.dict_in['timestamp']
        return self.dict_out

# toto bude content, ktory neskor budem vediet resolvovat aj
