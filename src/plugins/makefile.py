"""
Define all Makefile-specific properties here.

This is a bit of a hack... err... proof of concept, I mean ;-)
"""
import os

from owl import dag





class DAG(dag.DAG):
    def to_makefile(self):
        header = '\n'
        header += '# \n'
        header += '# Automatically generated by OWL\n'
        header += '# \n'
        phony = 'all: %s\n\n'
        phony += 'clean:\n'
        phony += '\trm -f %s\n\n'
        makefile = ''

        max_instances = max([n.job.Instances for n in self.nodes])

        # Turn the DAG into a Makefile. Try and understand what the final
        # dataset is and create a 'all' rule for it. That one should be the
        # output of the node with no children (true for simple cases).
        last = None
        all_outputs = ''
        for node in self.nodes:
            # Determine inputs and outputs
            input, output = dag._extract_inouts(node.job.Arguments)
            if(not node.children):
                last = output

            # Do job and argument expansion based on the number of instances.
            for i in range(node.job.Instances):
                # Make a copy.
                job_input = input.replace('$(Process)', str(i))
                job_output = output.replace('$(Process)', str(i))

                # Do the same processing to the full arg string.
                job_args = node.job.Arguments.replace('$(Process)', str(i))

                # FIXME: Does this happen if node.job.Instances == 1?
                if('%(ccdId)s' in job_input):
                    job_input = ' '.join([job_input % {'ccdId': j}
                                          for j in range(max_instances)])
                if('%(ccdId)s' in job_output):
                    job_output = ' '.join([job_output % {'ccdId': j}
                                           for j in range(max_instances)])

                makefile += job_output + ': ' + job_input + '\n'
                makefile += '\t%s %s\n\n' % (node.job.Executable,
                                             dag._escape(job_args))
                all_outputs += job_output + ' '

        # Now write the all rule.
        return(header + phony % (last, all_outputs) + makefile)



def submit(dagName, workDir):
    """
    Write out a makefile with all the necessary targets to execute the Workflow
    serially (or in parallel wherever possible using make -j N).
    """
    # All the files we need (the .dag file and the .job files) are in `workDir`
    # and have the names defined in the .dag file (which we are given as
    # `dagName`). So, first thing is to parse `dagName`.
    dag = DAG.new_from_classad(open(os.path.join(workDir, dagName)).read(), workDir)

    # Create the makefile
    f = open(os.path.join(workDir, 'Makefile'), 'w')
    f.write(dag.to_makefile())
    f.close()

    print('Makefile written in work directory %s' % (workDir))
    return(0)


