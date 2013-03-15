"""
ClassAd related classes and functions. ClassAds were introduced by the Condor
project but they have a life of their own. They are used to encapsulate job or
machine metadata (among others), perform queries and other niceties. They are a
bit like micro database engines, in a sense.

More information here: http://research.cs.wisc.edu/condor/classad/
"""
import re

import condorutils


# Constants
# We need this regex to replace line continuations with a space. In a ClassAd
# lines can be split over multiple lines using a \. When we find one, we get rid
# of it, append a space get the following like and stitch it to the current one.
MULTILINE_BUSTER = re.compile(' *\\\\ *\n *')



# Helper functions.
def parse_classad_environment(raw_value):
    """
    Given the raw value of the Environment CLassAd string, `raw_value`, parse it
    and return its Python dictionary equivalent. We only suppotrt new-style
    environment syntax (i.e. "key=val [key=val] ...").
    """
    env = {}

    env_str = raw_value.strip()
    if(env_str.startswith('"') and env_str.endswith('"')):
        env_str = env_str[1:-1]

    # Replace ' ' with a placeholder. We need to do that because the Condor
    # Environment string is a space-separated list of key=value pairs all
    # enclosed in double quotes. If value has a space in it, then the space has
    # to be single-quoted. Since we split on spaces, we want to avoid splitting
    # value strings and hence we replace "' '" with something else, split the
    # whole Environment string to get the list of key=value pairs and then
    # inside each value we put back that spaces (if needed).
    env_str = env_str.replace("' '", 'OWL_CONDOR_SPACE_SPLACEHOLDER')
    tokens = env_str.split()
    for token in tokens:
        # If this fails is because we have screwed up badly and we need to know.
        key, val = token.split('=', 1)
        env[key] = val.replace('OWL_CONDOR_SPACE_SPLACEHOLDER', ' ')
    return(env)



def _parse_classad_value(raw_value):
    """
    Parse a single ClassAd value and cast it to the appropriate Python type.
    """
    if(raw_value.startswith('"') and raw_value.endswith('"')):
        return(unicode(raw_value[1:-1]))
    if(raw_value.upper() == 'FALSE'):
        return(False)
    if(raw_value.upper() == 'TRUE'):
        return(True)
    try:
        return(int(raw_value))
    except (TypeError, ValueError):
        pass
    try:
        return(float(raw_value))
    except (TypeError, ValueError):
        pass
    try:
        return(unicode(raw_value))
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    raise(NotImplementedError('Unable to parse `%s`.' % (raw_value)))



def _parse(classad_text):
    """
    Given a multi-line ClassAd text, parse it and return the corresponding
        {key: val}
    dictionary.
    """
    # First of all, handle line continuations.
    classad_text = MULTILINE_BUSTER.sub(' ', classad_text)

    res = {}
    lines = classad_text.split('\n')
    for i in range(len(lines)):
        line = lines[i].strip()

        # Handle empty lines.
        if(not(line)):
            continue

        # Handle the Queue command, which does not have an = sign.
        if(line.lower().startswith('queue')):
            res['Instances'] = _extract_num_instances(line)
            continue

        # Handle simple, full line comments.
        if(line.startswith('#')):
            continue

        try:
            raw_key, raw_val = line.split('=', 1)
        except:
            raise(Exception('Cannot parse line "%s"' % (line)))

        # Remember to strip any leading + sign from the key name.
        key = raw_key.strip()
        if(key[0] == '+'):
            key = key[1:]
        val = _parse_classad_value(raw_val.strip())
        if(res.has_key(key)):
            raise(NotImplementedError('ClassAd arrays are not supported.'))
        res[key] = val
    return(res)


def _extract_num_instances(line):
    """
    Parse a Queue command and return the number of instances of the given Job to
    start on the grid. The default is 1. This assumes that the line being passed
    as input is indeed a Queue command. We also assume that the line has already
    been strip()-ed.
    """
    tokens = line.split()
    if(len(tokens) == 2):
        return(int(tokens[1]))
    return(1)


class ClassAd(object):
    """
    ClassAd container. Its attributes are dynamically set to match the input raw
    ClassAd text.
    """
    @classmethod
    def new_from_classad(cls, ad):
        """
        Given a Condor ClassAd text, parse it and create the corresponding Job
        instance. Save the raw ClassAd as _raw_classad instance variable for
        future reuse.
        """
        j = cls(**_parse(ad))
        j._raw_classad = ad
        return(j)

    def __init__(self, **kw):
        """
        Create a ClassAd object with a twist: since ClassAds are typically
        created by parsing a (case insensitive) user-supplied text description,
        we cannot expect them to follow any given case convention. For this
        reason we create aliases for each instance variable and allow access to
        be case-insensitive. This means that behind the scene, the lower-case
        version of the instance variable is always used.
        """
        map(lambda (k, v): setattr(self, k, v), kw.items())
        self._raw_classad = None
        return

    def __getattr__(self, name):
        if(name.islower()):
            raise(AttributeError("'%s' object has no attribute '%s'" \
                                 % (self.__class__.__name__, name)))
        return(getattr(self, name.lower()))

    def __setattr__(self, name, value):
        return(super(ClassAd, self).__setattr__(name.lower(), value))

    def __delattr__(self, name):
        return(super(ClassAd, self).__delattr__(name.lower()))

    def todict(self):
        """
        Convert to a simple dictionary.
        """
        return(self.__dict__)



class Job(ClassAd):
    """
    ClassAd subclass describing a Job i.e. a ClassAd with a MyType == Job. This
    involves making sure that some parameters/instance variables are present and
    giving standard defaults to the ones that are not. For a complete set of
    Job instance variables, see the man pages for condor_submit.
    """
    def __init__(self, **kw):
        super(Job, self).__init__(**kw)

        # When we are parsing a text file classad, MyType might not be defined.
        # In those cases, we set it to Job.
        if(not hasattr(self, 'MyType')):
            self.MyType = 'Job'

        # Just to be consistent, if JobState is undefined or None, set it to
        # 'Starting'. Just do this if self.MyType is Job
        if(self.MyType == 'Job' and
           ('JobState' not in kw.keys() or not kw['JobState'])):
            self.JobState = unicode('Starting')

        if(not hasattr(self, 'getenv')):
            self.getenv = False

        # Fix environment and add convenience environmentdict.
        if(not hasattr(self, 'environment')):
            self.environment = ''
        self.environmentdict = parse_classad_environment(self.environment)

        self._fix_dagman_job_id()
        return


    def _fix_dagman_job_id(self):
        """
        DAGManJobId is simply the parent DAGMan ClusterId, we would like to have
        it be a full GlobalJobId and here we try to infer the missing pieces of
        information (namely the hostname and submission timestamp) from the job
        classad.
        """
        # Remember that in Condor, ClusterIds start form 1, not 0. Also, if the
        # classad has a DAGManJobId, we assume that its MyType == Job
        dagman_job_id = getattr(self, 'DAGManJobId', None)

        # We can only fix DAGManJobId if we have CONDOR_PARENT_ID defined in the
        # job classad environment string.
        if(dagman_job_id and self.environmentdict):
            # parnt_id = submit_host:integer:timestamp
            parent_id = self.environmentdict.get('CONDOR_PARENT_ID', '')
            if(not parent_id):
                msg = 'CONDOR_PARENT_ID not defined in Job Environment string.'
                raise(Exception(msg))

            timestamp = parent_id.split(':')[-1]
            (host, _, _) = condorutils.parse_globaljobid(self.GlobalJobId)
            self.DAGManJobId = '%s#%s.0#%s' % (host, dagman_job_id, timestamp)
        return





























