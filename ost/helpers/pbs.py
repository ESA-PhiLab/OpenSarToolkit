# -*- coding: utf-8 -*-

def getPBSparams():
    
    print(' Please provide some information for the PBS launch of the jobs.')
    print(' An email address, where a notification of job completion will be send to.')
    mail = input(' Email: ')
    print(' Please provide your username of the PBS system (provided in a dedicated email)')
    PBSuname = input(' PBS username)
    print(' Please provide your IP of the PBS system (provided in a dedicated email)')
    PBSuname = input(' PBS username)
    print(' The directory of your main machine, where all necessary data for the job can be found.')
    remoteDIR = input ('Remote directory: ')
    
    
    return PBSuname, email, remoteDir, remoteIP 

def createPBSFile(Command, PBSFile, nodes=1, cpuTime='03:00:00'):


    PBSuname, email, remoteDir, remoteIP = getPBSparams()    
    
    
    file = open(PBSFile, 'w') 
 
    file.write('#PBS -N phiSAR')
    file.write('#PBS -l nodes={}'.format(nodes))
    file.write('#PBS -l cput={}'.format(cpuTime))
    file.write('PBS -M {}'.format(email))
    file.write('#PBS -m a')
    file.write('cd $PBS_O_WORKDIR')
   
    file.write('mkdir /home/00506_a0f8670/remote_${PBS_ARRAYID}') 
    file.write('sshfs eouser@{}:{} ${HOME}/remote_${PBS_ARRAYID} -o IdentityFile=${HOME}/key/.ssh.key')
     
    file.close() 
    
