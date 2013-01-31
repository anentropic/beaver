import celery
from functools import partial

from beaver.transport import Transport


class BaseCeleryTransport(Transport):

    def __init__(self, beaver_config, file_config, logger=None):
        super(CeleryTransport, self).__init__(beaver_config, file_config, logger=logger)
        # This is an abstract base class, need to supply a string or task object here:
        self.celery_task = None

    def callback(self, filename, lines):
        if isinstance(self.celery_task, basestring):
            task_method = partial(celery.send_task, self.celery_task)
        else:
            try:
                task_method = getattr(self.celery_task, 'apply_async')
            except AttributeError:
                raise NotImplemented('You must supply a Celery task object')

        lines = map(partial(self.format, filename=filename), lines)
        task_method(args=[lines])
