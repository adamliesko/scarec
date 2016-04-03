class Context:
    USABLE_PROPERTIES = []
    CLUSTERING_PROPERTIES = ['gender', 'age', 'income', 'browser', 'isp', 'os', 'geo_user', 'time_weekday',
                             'do_not_track', 'weather_kind', 'weather', 'geo_publisher', 'lang_user',
                             'time_to_action', 'geo_user_zip', 'time_hour', 'device_type', 'geo_type']

    MAPPINGS = {
        '57': 'user_id',
        '27': 'publisher_id',
        '25': 'item_id',
        '11': 'category_id',
        '10': 'channel_id',
        '1': 'gender',
        '2': 'age',
        '3': 'income',
        '4': 'browser',
        '5': 'isp',
        '6': 'os',
        '7': 'geo_user',
        '9': 'time_weekday',
        '13': 'do_not_track',
        '14': 'weather_kind',
        '15': 'weather',
        '16': 'geo_publisher',
        '17': 'lang_user',
        '20': 'time_to_action',
        '22': 'geo_user_zip',
        '23': 'time_hour',
        '47': 'device_type',
        '48': 'geo_type'
    }

    MAPPINGS_INV = inv_map = {v: k for k, v in MAPPINGS.items()}

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

    @classmethod
    def context_to_clustering_input(cls, context):
        return ContextEncoder.encode_context_to_vec(context)
