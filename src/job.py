# Copyright (C) 2010 Association of Universities for Research in Astronomy(AURA)
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#     1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
# 
#     2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
# 
#     3. The name of AURA and its representatives may not be used to
#       endorse or promote products derived from this software without
#       specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY AURA ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AURA BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
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
        if(line.lower().split()[0] == 'queue'):
            # FIXME: Do handle this.
            continue
        
        # Handle simple, full line comments.
        if(line.startswith('#')):
            continue
        
        try:
            rawKey, rawVal = line.split('=', 1)
        except:
            raise(Exception('Cannot parse line "%s"' % (line)))
        
        key = rawKey.strip()
        val = _parseClassAdValue(rawVal.strip())
        if(res.has_key(key)):
            raise(NotImplementedError('ClassAd arrays are not supported.'))
        res[key] = val
    return(res)
        


class Job(object):
    @classmethod
    def newFromClassAd(cls, ad):
        """
        Given a Condor ClassAd text, parse it and create the corresponding Job
        instance.
        """
        return(cls(**_parse(ad)))
    
    def __init__(self, **kw):
        map(lambda (k, v): setattr(self, k, v), kw.items())
        
        # Just to be consistent, if JobState is undefined or None, set it to
        # 'Starting'.
        if('JobState' not in kw.keys() or not kw['JobState']):
            self.JobState = unicode('Starting')
        return
    
    def updateFromClassAd(self, ad):
        """
        Update the Job instance variables from the given raw ClassAd. Return the
        instance variable names that were updated, their old value and the new
        value in the form
            {var: old}
        if an instance variable was added, then we only have
            {var: None}
        which clearly introduces a possible degeneracy in the case the variable 
        existed, was set to None and then updated to a new value vs the case
        where the variable did not exist. Also we get the same behaviour in the 
        case where the variable did not exists and is now set to None.
        """
        updated = {}
        
        newClassAd = _parse(ad)
        for key, val in newClassAd.items():
            if(getattr(self, key, None) != val or not hasattr(self, key)):
                updated[key] = getattr(self, key)
                setattr(self, key, val)
        return(updated)
    
        
        




















