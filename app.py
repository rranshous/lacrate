

class BunchProcess(Process):
    """
    process which executes the handlers on the bunch
    """

    def __init__(self, handler_bunch, event_queue, stopping):

        # respect
        super(BunchProcess,self).__init__()

        # keep our bunch
        self.handler_bunch = handler_bunch

        # our events to handle
        self.event_queue = event_queue

        # need to know when to stop
        self.stopping = stopping

    def run(self):
        """
        sit on the bunch's event queue passing
        events to the handlers
        """

        while not self.stopping.is_set():
            try:
                # wait around a bit waiting for a new event to handle
                event_name, event_data = self.event_queue.get(True, 60)
            except Empty:
                continue

            # run our event handlers against this event
            self.handler_bunch.run_handlers(event_name, event_data)

class HandlerProcessPool(Thread):
    """
    manages processes which run handlers for events
    """

    # i think what we are going to do is spawn up a process per
    # handler bunch as work for that bunch is received.
    # the process will live for a while waiting for more work,
    # and if it doesn't receive it will die and be collected

    def __init__(self, activation_queue, stopping, handler_bunches):

        # respect
        super(HandleProcessPool,self).__init__()

        # queue of bunches which have just received events and need
        # to be activated
        self.activation_queue = activation_queue

        # handler bunches which we are going to manage procs for
        self.handler_bunches = handler_bunches

        # build a lookup of handler bunches by channel name
        self.bunch_lookup = {}
        self._update_bunch_lookup()

    def run(self):

        # now we want to sit on the activation queue
        # and make sure that processes are started for
        # all channel's which are getting work
        while not stopping.is_set():
            try:
                channel = self.activation_queue.get(True,60)
            except Empty, ex:
                continue

            # make sure the channel is active
            bunch = self.bunch_lookup.get(channel)
            if not bunch.active.is_set():
                self._activate_bunch(bunch)

    def _activate_bunch(self, bunch):
        """
        spawns up a handler process for the bunch
        """
        pass # TODO

    def _update_bunch_lookup(self):
        for bunch in self.handler_bunches:
            self.bunch_lookup[bunch.channel] = bunch



class ReventListenerThreadPool(object):
    """
    thread pool which has additional add method
    which adds a revent listening thread
    """

    def __init__(self, stopping, revent_config):

        self.stopping = stopping
        self.threads = []

        # setup our redis client, if we don't already have one
        if not revent_config.get('redis_client'):
            redis_client = Redis(revent_config.get('redis_host'),
                                 db = revent_config.get('redis_db'),
                                 password = revent_config.get('redis_password'))
            revent_config['redis_client' ] = redis_client

    def add(self, channel, events, events_queue):
        """
        adds a revent listener thread
        """

        # update this threads revent config for it's channel / events
        revent_config = self.revent_config.copy()
        revent_config.update({
            'events':events,
            'channel':channel
        })

        # start our handling thread
        args = self.stopping, events_queue, revent_config
        thread = self._add_thread(self._revent_listener, args)

    def _add_thread(self, func, args, kwargs):
        """
        starts a new thread, using given func args + kwargs
        """

        # creat the thread
        thread = Thread(target = func,
                        args = args,
                        kwargs = kwargs)

        # thrack it
        self.threads.append(thread)

        # we'll get it started
        thread.start()

        return thread

    @static
    def _revent_listener(stopping, event_queue, revent_config):

        # setup revent client
        revent = ReventClient(revent_config)

        # listen for our events
        while not stopping.is_set():
            event_name, event_data = revent.get_event()
            if event_name
                event_queue.put((event_name,event_data))


def get_callables(module):
    attrs = dir(module)
    for attr in attrs:
        v = getattr(module,attr)
        if callable(v):
            yield v

class HandlerBunch(object):
    def __init__(self, module, revent_config):

        # grab our module
        self.module = module

        # keep our revent configuration
        self.revent_config = revent_config

    def run_handlers(self, event_name, event_data):
        """
        runs the passed event info through all the handlers
        """

        # update the handlers, injecting in deps
        self._inject_handler_deps()

        # we are running all the handlers
        for handler in self.handlers:

            # for now we're not going to care what
            # happens in the handler, return True, return False
            # raise an exception, I don't care. Im going to keep going
            try:
                r = handler(event_name, event_data)
            except Exception, ex:
                # fuck your exception
                pass

        # for now, return True # TODO: better
        return True

    def _inject_handler_deps():
        """
        injects the deps into the handlers
        """

        # all the handlers are going to be running
        # in the same process
        log = logging.getLogger(self.channel)
        config = get_channel_config(self.channel)
        revent = ReventClient(self.revent_config)

        # all the handler's need their deps
        for handler in self.handlers:

            # inject a logger namespaced to the channel
            handler.log = log

            # smack in this channel's config
            handler.config = config

            # give the handler
            handler.revent = revent

    def _get_handlers(self):
        """
        returns an iterator for the handlers in this bunch
        """
        for _callable in get_callables(self.module)
            # check it's name
            if _callable.__name__.lower().startswith('handler'):
                yield callable

    # handler iterator
    handlers = property(_get_handlers)

    # pull the channel off the module
    channel = property(lambda s: s.module.channel)



def get_hander_paths(handler_root_path):

    # find all the python files off the root
    python_files = []
    for file_path in findfiles(handler_root_path, extension='.py'):
        yield file_path

def get_handler_bunches(handler_root_path):

    # grab the paths to our modules
    handler_paths = get_handler_paths(handler_root_path)

    # go through importing the modules
    for path in handler_paths:

        # grab the module
        module = __import__(path)

        # instantiate our bunch and lettr fly
        bunch = HandlerBunch(module)
        yield bunch

# TODO: make less ugly
def normalize_paths(virtualenv_path,
                    python_path,
                    handler_root_path,
                    config_root_path):

    # make sure we have a good handler root
    assert handler_root_path, "Must specify handlers path"
    assert exists(handler_root_path), ("Handler root path does not exist: %s"
                                       % handler_root_path)

    # if we didn't get a python path make it handler root
    python_path = python_path or handler_root_path

    # if we didn't get a config root, find one
    if not config_root_path:
        # the config root path is going to be a configs
        # dir in root or the root itself
        possible_config_path = path_join(handler_root_path,'configs')
        if exists(possible_config_path):
            config_root_path = possible_config_path

        # if there is no config dir, than it's the handler root
        else:
            config_root_path = handler_root_path

    assert exists(config_root_path), ("Config path does not exist: %s"
                                      % config_root_path)

    return virtualenv_path, python_path, handler_root_path, config_root_path

def run():

    # our stopping flag
    stopping = Lock()

    # we'll get these from some where else
    virtualenv_path = ''
    python_path = ''
    handler_root_path = ''
    config_root_path = ''

    # update our paths
    # TODO: make less ugly, too lazy ATM
    (virtualenv_path,
     python_path,
     handler_root_path,
     config_root_path ) = normalize_paths(virtualenv_path,
                                          python_path,
                                          handler_root_path,
                                          config_root_path)


    # TODO: handle virtualenv path
    # update our path respecting virtualenv

    # update our python path to match the one provided

    # read in the bunches
    handler_bunches = get_handler_bunches(handler_root_path)

    # read in their configs

    # instantiate our revent listeners, we need one revent client
    # per channel
    revent_listener_pool = ReventListenerThreadPool(stopping,
                                                    revent_config)
    for handler_bunch in handler_bunches:
        # add a queue for events to be handled
        handler_bunch.events_in_queue = Queue()
        # setup a queue on each bunch for it's events
        ReventListenerThreadPool.add(handler_bunch.channel,
                                     handler_bunch.events,
                                     handler_bunch.events_in_queue)

    # now setup our process pool for the handlers
    # it feeds off the event in queue
    handler_process_pool = HandlerProcessPool(event_in_queue, stopping, handler_bunches)

    # now we wait
    while not stopping.is_set()
        sleep(1)

    # we're stopping !

    # make sure our Revent listener pool has closed it's threads
    # TODO

    # make sure the Handler Processes are all done
    # TODO

    # done





