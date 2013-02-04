import celery

from beaver.transport import Transport


class CeleryTransport(Transport):
    # need to supply a string (dotted import path) or celery task object here:
    celery_task = None

    def callback(self, filename, lines):
        if isinstance(self.celery_task, basestring):
            task_method = partial(celery.send_task, self.celery_task)
        else:
            try:
                task_method = getattr(self.celery_task, 'apply_async')
            except AttributeError:
                raise NotImplemented('You must supply a Celery task object')

        lines = [self.format(filename, line) for line in lines]
        task_method(args=[lines])
