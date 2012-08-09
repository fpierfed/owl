"""
Define all Makefile-specific properties here.

This is a bit of a hack... err... proof of concept, I mean ;-)
"""
import os

import jinja2

from owl import dag




# Constants
NATIVE_SPEC_TMPL = jinja2.Template('''\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<array>
    <dict>
        <!-- A symbolic name of the job -->
        <key>name</key>
        <string>{{ dataset }}</string>

        <key>schedulerParameters</key>
        <dict>
            <!-- do all of the given tasks need to start at the same time? -->
            <key>tasksMustStartSimultaneously</key>
            <string>NO</string>

            <!-- what's the minimum number that need to start at the same time? -->
            <key>minimumTaskCount</key>
            <integer>1</integer>
        </dict>

        <!-- specifications of all tasks of this job -->
        <key>taskSpecifications</key>
        <dict>
            {% for task in tasks %}
            <!-- key is symbolic task name -->
            <key>{{ task.id }}</key>
            <dict>
                <!-- command to execute -->
                <key>command</key>
                <string>{{ task.command }}</string>

                <!-- argument array -->
                <key>arguments</key>
                <array>
                    {% for arg in task.args -%}
                    <string>{{ arg }}</string>
                    {% endfor %}
                </array>

                {% if task.parent_ids is sequence %}
                <!-- do not start this task until the following tasks (symbolic names) have finished successfully -->
                <key>dependsOnTasks</key>
                <array>
                    {% for parent_id in task.parent_ids -%}
                    <string>{{ parent_id }}</string>
                    {% endfor %}
                </array>
                {% endif %}
            </dict>
            {% endfor %}
        </dict>
    </dict>
</array>
</plist>

''')




class DAG(dag.DAG):
    def to_xgrid_plist(self, dataset):
        # Turn the DAG into an XGrig plist. Keep track of tasks/jobs.
        tasks = []
        for node in self.nodes:
            # Get the parent IDs.
            parent_ids = []
            for parent in node.parents:
                if(parent.job.Instances > 1):
                    for j in range(parent.job.Instances):
                        parent_ids.append(parent.name + '_' + str(j))
                else:
                    parent_ids.append(parent.name)
            if(not parent_ids):
                parent_ids = None

            if(node.job.Instances > 1):
                for i in range(node.job.Instances):
                    # Fix the arguments.
                    args = node.job.Arguments.replace('$(Process)', str(i))

                    tasks.append({'id': node.name + '_' + str(i),
                                  'command': node.job.Executable,
                                  'args': args.split(),
                                  'parent_ids': parent_ids})
            else:
                tasks.append({'id': node.name,
                              'command': node.job.Executable,
                              'args': node.job.Arguments.split(),
                              'parent_ids': parent_ids})



        # Now write the plist.
        return(NATIVE_SPEC_TMPL.render(dataset=dataset, tasks=tasks))



def submit(dagName, workDir):
    """
    Write out a makefile with all the necessary targets to execute the Workflow
    serially (or in parallel wherever possible using make -j N).
    """
    # All the files we need (the .dag file and the .job files) are in `workDir`
    # and have the names defined in the .dag file (which we are given as
    # `dagName`). So, first thing is to parse `dagName`.
    dag = DAG.new_from_classad(open(os.path.join(workDir, dagName)).read(), workDir)

    # Extract the dataset name. We assume dagName = dataset.dag
    dataset, ext = os.path.splitext(dagName)

    # Create the XGrid plist
    f = open(os.path.join(workDir, dataset + '.plist'), 'w')
    f.write(dag.to_xgrid_plist(dataset))
    f.close()

    print('XGrid batch job file written in work directory %s' % (workDir))
    return(0)


