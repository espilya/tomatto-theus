import paramiko
import os
import subprocess
import re
"""
mem usage:
free -m | awk 'NR==2{print 'Memory Usage: ' $3 '/' $2 'MB (' $3/$2*100 "%)'}'
free -m | awk 'NR==2{print $3/$2*100}'

cpu usage:
top -bn1 | grep 'Cpu(s)' | sed 's/.*, *\([0-9.]*\)%* id.*/\1/' | awk '{print 'CPU Usage: ' 100 - $1 '%'}'
top -bn1 | grep 'Cpu(s)' | sed 's/.*, *\([0-9.]*\)%* id.*/\1/' | awk '{print 100 - $1}'
          		   sed 's/.*, *\([0-9.]*\)%* id.*/\1/'

old: pdsh -w {} ps -u {} -o pid,state,cputime,%cpu,%mem,vsize,rssize,command --columns 100
"""
def parser(data):
    result = {}
    lines = data.strip().split("\n")
#    print(data)
    # Process each line
    for line in lines:
        jobid, param, value = line.split(':')
        jobid = jobid.strip()  # Clean up jobid if necessary
        param = param.strip()  # Clean up parameter
        value = float(value.strip())  # Convert value to float for numbers

        # Initialize the jobid entry if it doesn't exist
        if jobid not in result:
            result[jobid] = {}

        # Add the parameter (mem or cpu) and its value to the corresponding jobid
        result[jobid][param] = value
    return result

def parse_hosts(strings):
    #print(strings)
    result = []
    for s in strings:
        # Extract the base string and the ranges
        match = re.match(r"(\w+)\[(.+)\]", s)
        if not match:
            continue
        base_str = match.group(1)
        ranges = match.group(2).split(',')
        for r in ranges:
            if '-' in r:
                # Handle range
                start, end = map(int, r.split('-'))
                result.extend([f"{base_str}{i:02}" for i in range(start, end + 1)])
            else:
                # Handle single number
                result.append(f"{base_str}{int(r):02}")
    return result

def read_metrics(hosts):
    #print(hosts)
    group = []
    individual = []
    for i in hosts:
        if '[' in i:
            group.append(i)
        else:
            individual.append(i)
    
    p_group = []
    if group != []:
        p_group = parse_hosts(group)

    hosts = individual + p_group
    hosts_str = ",".join(hosts)
    #print(hosts_str)

    cmd1= "echo -n mem: && free -m | awk 'NR==2{print \$3 / \$2*100}'"
    cmd2= "echo -n cpu: && top -bn1 | grep 'Cpu(s)' | sed 's/.*, *\([0-9.]*\)%* id.*/\\1/' | awk '{print 100 - \$1}'"
    cmdfull= 'pdsh -w {} "{} && {}"'.format(hosts_str, cmd1, cmd2)
#    print(cmdfull)
    result = subprocess.run(cmdfull, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    r = result.stdout.decode('utf-8')
    print(r)
    parsed_r = parser(r)
#    print(parsed_r)
    return parsed_r
    
    commands = [
        "free -m | awk 'NR==2{print $3/$2*100}'",  # Memory usage
        "top -bn1 | grep 'Cpu(s)' | sed 's/.*, *\([0-9.]*\)%* id.*/\1/' | awk '{print 100 - $1}'"  # CPU usage
    ]
    # Create SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    mykey = paramiko.RSAKey.from_private_key_file(os.path.expanduser('~/.ssh/cluster'))
    try:
        # Connect to the remote host
        ssh.connect(hostname=machine_name, username='ilya', pkey=mykey)

        # Run the commands and capture the output
        memory_usage = ssh.exec_command(commands[0])[1].read().decode('utf-8').strip()
        cpu_usage = ssh.exec_command(commands[1])[1].read().decode('utf-8').strip()

        # Print the results
 #       print(memory_usage)
 #       print(cpu_usage)

    finally:
        ssh.close()

