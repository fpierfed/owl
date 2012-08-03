import re




# Constants
MULTILINE_BUSTER = re.compile(' *\\\\ *\n *')



# Helper functions.
def _parseClassAdValue(rawVal):
    if(rawVal.startswith('"') and rawVal.endswith('"')):
        return(unicode(rawVal[1:-1]))
    if(rawVal.upper() == 'FALSE'):
        return(False)
    if(rawVal.upper() == 'TRUE'):
        return(True)
    try:
        return(int(rawVal))
    except:
        pass
    try:
        return(float(rawVal))
    except:
        pass
    try:
        return(unicode(rawVal))
    except:
        pass
    raise(NotImplementedError('Unable to parse `%s`.' % (rawVal)))



def _parse(classAdText):
    """
    Given a multi-line ClassAd text, parse it and return the corresponding
        {key: val}
    dictionary.
    """
    # First of all, handle line continuations.
    classAdText = MULTILINE_BUSTER.sub(' ', classAdText)

    res = {}
    lines = classAdText.split('\n')
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
            rawKey, rawVal = line.split('=', 1)
        except:
            raise(Exception('Cannot parse line "%s"' % (line)))

        # Remember to strip any leading + sign from the key name.
        key = rawKey.strip()
        if(key[0] == '+'):
            key = key[1:]
        val = _parseClassAdValue(rawVal.strip())
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
    @classmethod
    def newFromClassAd(cls, ad):
        """
        Given a Condor ClassAd text, parse it and create the corresponding Job
        instance. Save the raw ClassAd as _rawClassAd instance variable for
        future reuse.
        """
        j = cls(**_parse(ad))
        j._rawClassAd = ad
        return(j)

    def __init__(self, **kw):
        map(lambda (k, v): setattr(self, k, v), kw.items())

        # Just to be consistent, if JobState is undefined or None, set it to
        # 'Starting'. Just do this if self.MyType is Job
        if(self.MyType == 'Job' and
           ('JobState' not in kw.keys() or not kw['JobState'])):
            self.JobState = unicode('Starting')
        return

    def todict(self):
        """
        Convert to a simple dictionary.
        """
        return(self.__dict__)



class Job(ClassAd):
    pass



























