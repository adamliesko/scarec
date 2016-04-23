import falcon


# Falcon follows the REST architectural style, meaning (among
# other things) that you think in terms of resources and state
# transitions, which map to HTTP verbs.
class ErrorsResource:
    def on_post(self, req, resp):
        body = req.stream.read()
        # logger.warn('Received an error from plista api:' + body)
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = ('Ok, so what :)')
