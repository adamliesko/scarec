from __future__ import print_function

import sys
import os
import nose

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from ..api import AlchemyAPI, Auth

__all__ = ['TestAPI']

test_text = 'Bob broke my heart, and then made up this silly sentence to test the PHP SDK'
test_html = '<html><head><title>The best SDK Test | AlchemyAPI</title></head><body><h1>Hello World!</h1><p>My favorite language is PHP</p></body></html>'
test_url = 'http://www.nytimes.com/2013/07/13/us/politics/a-day-of-friction-notable-even-for-a-fractious-congress.html?_r=0'


class TestAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        key = os.environ.get('ALCHEMY_API_KEY', None)
        cls._api = AlchemyAPI(Auth(key))

    # Entities
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_entities(self):
        print('Checking entities . . . ')
        response = self._api.interface('entities', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('entities', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('entities', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('entities', 'random', test_url)
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        print('Entity tests complete!')
        print('')


    # Keywords
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_keywords(self):
        print('Checking keywords . . . ')
        response = self._api.interface('keywords', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('keywords', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('keywords', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('keywords', 'random', test_url)
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        print('Keyword tests complete!')
        print('')


    # Concepts
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_concepts(self):
        print('Checking concepts . . . ')
        response = self._api.interface('concepts', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('concepts', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('concepts', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('concepts', 'random', test_url)
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        print('Concept tests complete!')
        print('')


    # Sentiment
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_sentiment(self):
        print('Checking sentiment . . . ')
        response = self._api.interface('sentiment', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('sentiment', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('sentiment', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('sentiment', 'random', test_url)
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        print('Sentiment tests complete!')
        print('')


    # Targeted Sentiment
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_target_sentiment(self):
        print('Checking targeted sentiment . . . ')
        response = self._api.interface('sentiment_targeted', 'text', test_text, target='heart')
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('sentiment_targeted', 'html', test_html, target='language')
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('sentiment_targeted', 'url', test_url, target='Congress')
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('sentiment_targeted', 'random', test_url, target='Congress')
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        response = self._api.interface('sentiment_targeted', 'text', test_text,  target=None)
        self.assertTrue(response['status'] == 'ERROR')  # missing target
        print('Targeted sentiment tests complete!')
        print('')


    # Text
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_text(self):
        print('Checking text . . . ')
        response = self._api.interface('text', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')  # only works for html and url content
        response = self._api.interface('text', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('text', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Text tests complete!')
        print('')


    # Text Raw
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_raw(self):
        print('Checking raw text . . . ')
        response = self._api.interface('text_raw', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')  # only works for html and url content
        response = self._api.interface('text_raw', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('text_raw', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Raw text tests complete!')
        print('')


    # Author
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_author(self):
        print('Checking author . . . ')
        response = self._api.interface('author', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')  # only works for html and url content
        response = self._api.interface('author', 'html', test_html)
        self.assertTrue(response['status'] == 'ERROR')  # there's no author in the test HTML
        response = self._api.interface('author', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Author tests complete!')
        print('')


    # Language
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_language(self):
        print('Checking language . . . ')
        response = self._api.interface('language', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('language', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('language', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('language', 'random', test_url)
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        print('Language tests complete!')
        print('')


    # Title
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_title(self):
        print('Checking title . . . ')
        response = self._api.interface('title', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')  # only works for html and url content
        response = self._api.interface('title', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('title', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Title tests complete!')
        print('')


    # Relations
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_relation(self):
        print('Checking relations . . . ')
        response = self._api.interface('relations', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('relations', 'html', test_html)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('relations', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('relations', 'random', test_url)
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        print('Relation tests complete!')
        print('')


    # Category
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_category(self):
        print('Checking category . . . ')
        response = self._api.interface('category', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('category', 'html', test_html, url='test')
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('category', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('category', 'random', test_url)
        self.assertTrue(response['status'] == 'ERROR')  # invalid flavor
        print('Category tests complete!')
        print('')


    # Feeds
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_feeds(self):
        print('Checking feeds . . . ')
        response = self._api.interface('feeds', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')  # only works for html and url content
        response = self._api.interface('feeds', 'html', test_html, url='test')
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('feeds', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Feed tests complete!')
        print('')


    # Microformats
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_microformats(self):
        print('Checking microformats . . . ')
        response = self._api.interface('microformats', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')  # only works for html and url content
        response = self._api.interface('microformats', 'html', test_html, url='test')
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('microformats', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Microformat tests complete!')
        print('')
        print('')

    @unittest.skip('Skipping image_tagging')
    def test_imagetagging(self):
        print('Checking imagetagging . . . ')
        response = self._api.interface('image_tagging', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')
        response = self._api.interface('image_tagging', 'html', test_html)
        self.assertTrue(response['status'] == 'ERROR')
        response = self._api.interface('image_tagging', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('image_tagging', 'image', test_jpg, image=test_jpg, imagePostMode='raw')
        self.assertTrue(response['status'] == 'OK')
        print('Image tagging tests complete!')
        print('')
        print('')

    # combined
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_combined(self):
        print('Checking combined . . . ')
        response = self._api.interface('combined', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('combined', 'html', test_html)
        self.assertTrue(response['status'] == 'ERROR')
        response = self._api.interface('combined', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Combined tests complete!')
        print('')
        print('')

    # taxonomy
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_taxonomy(self):
        print('Checking taxonomy . . . ')
        response = self._api.interface('taxonomy', 'text', test_text)
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('taxonomy', 'html', test_html, url='test')
        self.assertTrue(response['status'] == 'OK')
        response = self._api.interface('taxonomy', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Taxonomy tests complete!')
        print('')
        print('')

    # image
    @unittest.skipIf(not os.environ.get('ALCHEMY_API_KEY', None), 'No API Key')
    def test_image_extraction(self):
        print('Checking image extraction . . . ')
        response = self._api.interface('image', 'text', test_text)
        self.assertTrue(response['status'] == 'ERROR')
        response = self._api.interface('image', 'html', test_html)
        self.assertTrue(response['status'] == 'ERROR')
        response = self._api.interface('image', 'url', test_url)
        self.assertTrue(response['status'] == 'OK')
        print('Image Extraction tests complete!')
        print('')
        print('')


if __name__ == '__main__':
    _stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')
    nose.main()
    sys.stdout = _stdout
    print('**** All tests complete! ****')
