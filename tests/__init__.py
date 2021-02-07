import functools

import mock


def patch_invalidations(func):
    """
    Patches cache.enqueue_invalidations where the backend is not a mysql instance.

    :param func: The function to decorate
    :return: The wrapped function
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with mock.patch('monetate.retailer.cache.invalidation_context', autospec=True), \
             mock.patch('monetate.retailer.cache.enqueue_invalidations', autospec=True), \
             mock.patch('monetate.retailer.models.invalidate_bucket', autospec=True):
            return func(*args, **kwargs)

    return wrapper
