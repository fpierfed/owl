"""
ClassAd related classes and functions. ClassAds were introduced by the Condor
project but they have a life of their own. They are used to encapsulate job or
machine metadata (among others), perform queries and other niceties. They are a
bit like micro database engines, in a sense.

More information here: http://research.cs.wisc.edu/condor/classad/
"""
import re




# Constants
MULTILINE_BUSTER = re.compile(' *\\\\ *\n *')



# Helper functions.
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
        map(lambda (k, v): setattr(self, k, v), kw.items())

        # Just to be consistent, if JobState is undefined or None, set it to
        # 'Starting'. Just do this if self.MyType is Job
        if(self.MyType == 'Job' and
           ('JobState' not in kw.keys() or not kw['JobState'])):
            self.JobState = unicode('Starting')

        self._raw_classad = None
        return

    def todict(self):
        """
        Convert to a simple dictionary.
        """
        return(self.__dict__)



class Job(ClassAd):
    """
    Alias for ClassAd.
    """
    pass



























