from beaver.transport import Transport


class CeleryTransport(Transport):
    # need to supply a string (dotted import path) or celery task object here:
    celery_task = None
    # your instantiated Celery() app (required to use string above)
    celery_app = None

    def callback(self, filename, lines):
        if isinstance(self.celery_task, basestring) and self.celery_app is not None:
            task_method = partial(self.celery_app.send_task, self.celery_task)
        else:
            try:
                task_method = getattr(self.celery_task, 'apply_async')
            except AttributeError:
                raise NotImplemented('You must supply a Celery task object')

        lines = [self.format(filename, line) for line in lines]
        # beaver ignores return values but it helps for testing
        return task_method(args=[lines])
