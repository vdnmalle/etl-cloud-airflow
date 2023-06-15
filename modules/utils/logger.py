import logging


class logger:
    def _console_log(self, msg, error=False):
        if not error:
            logging.info("{} job ==========>  \n".format(msg))
        else:
            logging.error("{} job ==========> \n".format(msg))

    def debug_log(self,body):
        body = body.getvalue()
        # write logic for each cloud by extending the class and overriding it
