import redis_client

redis_client.get_client().set('12', 'foo')
redis_client.get_client().get('12')

class Item:
    a = {"domainid":"418","created_at":"2013-06-01 09:23:52","flag":0,"img":"http:\/\/www.ksta.de\/image\/view\/23088116,19853145,lowRes,urn-newsml-dpa-com-20090101-130601-99-00642_large_4_3.jpg","title":"NBA: Drew neuer Trainer bei den Milwaukee Bucks","text":"Larry Drew ist neuer Trainer bei den Milwaukee Bucks. Das teilte der Basketball-Club aus der nordamerikanischen Profiliga ...","url":"http:\/\/www.ksta.de\/sport\/nba--drew-neuer-trainer-bei-den-milwaukee-bucks,15189364,23088118.html","expires_at":1385623432,"published_at": None,"version":1,"updated_at":"2013-06-01 23:40:59","id":"128053708"}



    print(a['domainid'])