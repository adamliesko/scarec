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
        'publisher_id': '27'
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
        return self.dict_out


req = {"type": "impression", "context": {
    "simple": {"27": 596, "25": 128210663, "4": 7, "56": 1138207, "29": 17332, "52": 1, "14": 33331, "39": 970,
               "16": 48811, "7": 18851, "42": 0, "19": 78119, "24": 12, "6": 9, "5": 6, "47": 654013, "18": 0,
               "37": 1205425, "17": 48985, "22": 69613, "31": 0, "13": 2, "9": 26889, "23": 23, "49": 47,
               "35": 315003,
               "57": 1877421652}, "lists": {"8": [18841, 18842], "10": [6, 13], "11": [1201425]}, "clusters": {
        "33": {"491611": 8, "553567": 6, "595102": 3, "129052": 2, "44507": 1, "3869887": 1, "739179": 1,
               "2167572": 1,
               "464068": 1, "40853": 1, "37192": 1, "19899293": 1, "7910": 0, "398": 0, "50327": 0, "15858": 0,
               "2844": 0}, "2": [21, 11, 50, 74, 60, 23, 11], "46": {"472376": 255, "472375": 255, "472358": 255},
        "51": {"3": 255}, "1": {"7": 255}, "3": [50, 28, 34, 98, 28, 15]}},
       "recs": {"ints": {"3": [128198131, 103436778, 128166628, 128181359, 128116218, 123189685]}},
       "timestamp": 1370296799113}

abc = Context(req)
print(abc.extract_to_json())


# toto bude content, ktory neskor budem vediet resolvovat aj
