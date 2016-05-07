import falcon


class ErrorsResource:
    def on_post(self, req, resp):
        body = req.stream.read()
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = ('Ok, so what will we do with th error?')
