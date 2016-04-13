/* 
* Copyright (C) 2015-2016 Quantum HPC Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

var cproc = require('child_process');
var spawn = cproc.spawnSync;
var fs = require("fs");
var path = require("path");
var nodeStatus = {'ok' : 'OK', 'unavail' : 'Unavailable', 'unreach' : 'Unreachable', 'closed' : 'Closed'};

// General command dictionnary keeping track of implemented features
var cmdDict = {
    "queue"    :   "",
    "queues"   :   "",
    "job"      :   "",
    "jobs"     :   "",
    "node"     :   "",
    "nodes"    :   "bhosts -l",
    "submit"   :   "",
    "delete"   :   "",
    "setting"  :   "",
    "settings" :   ""
    };
    
    
// Parse the command and return stdout of the process depending on the method
/*
    spawnCmd                :   shell command   /   [file, destinationDir], 
    spawnType               :   shell           /   copy, 
    spawnDirection          :   null            /   send || retrieve, 
    lava_config
*/
// TODO: treat errors
function spawnProcess(spawnCmd, spawnType, spawnDirection, lava_config){
    var spawnExec;
    switch (spawnType){
        case "shell":
            switch (lava_config.method){
                case "ssh":
                    spawnExec = lava_config.ssh_exec;
                    spawnCmd = [lava_config.username + "@" + lava_config.serverName,"-o","StrictHostKeyChecking=no","-i",lava_config.secretAccessKey].concat(spawnCmd.split(" "));
                    break;
                case "local":
                    spawnExec = lava_config.local_shell;
                    spawnCmd = spawnCmd.split(" ");
                    break; 
            }
            break;
        //Copy the files according to the spawnCmd array : 0 is the file, 1 is the destination dir
        case "copy":
            // Special case if we can use a shared file system
            if (lava_config.useSharedDir){
                spawnExec = lava_config.local_copy;
            }else{
                switch (lava_config.method){
                    // Build the scp command
                    case "ssh":
                        spawnExec = lava_config.scp_exec;
                        var file;
                        var destDir;
                        switch (spawnDirection){
                            case "send":
                                file    = spawnCmd[0];
                                destDir = lava_config.username + "@" + lava_config.serverName + ":" + spawnCmd[1];
                                break;
                            case "retrieve":
                                file    = lava_config.username + "@" + lava_config.serverName + ":" + spawnCmd[0];
                                destDir = spawnCmd[1];
                                break;
                        }
                        spawnCmd = ["-o","StrictHostKeyChecking=no","-i",lava_config.secretAccessKey,file,destDir];
                        break;
                    case "local":
                        spawnExec = lava_config.local_copy;
                        break; 
                }
            }
            break;
    }
    return spawn(spawnExec, spawnCmd, { encoding : 'utf8' });
}

function createUID()
{
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random()*16|0, v = c === 'x' ? r : (r&0x3|0x8);
        return v.toString(16);
    });
}

function createJobWorkDir(lava_config){
    // Get configuration working directory and Generate a UID for the working dir
    var jobWorkingDir = path.join(lava_config.working_dir,createUID());
    
    //Create workdir
    spawnProcess("[ -d "+jobWorkingDir+" ] || mkdir "+jobWorkingDir,"shell", null, lava_config);
    
    //TODO:handles error
    return jobWorkingDir;
}

function jsonifyBhosts(output){
    var results={};
    // Store node name
    results.name = output.shift().trim();
    
    // Look for properties, first line is labels
    // Second line is Status
    // for (var i = 1; i < output.length; i++) {
    var data = output[1].split(/\s+/g);
    results.status      =   nodeStatus[data[0]];
    results.cpuf        =   (data[1] === '-' ? null : data[1]);
    results.jlu         =   (data[2] === '-' ? null : data[2]);
    results.max         =   (data[3] === '-' ? null : data[3]);
    results.njobs       =   (data[4] === '-' ? null : data[4]);
    results.run         =   (data[5] === '-' ? null : data[5]);
    results.ssusp       =   (data[6] === '-' ? null : data[6]);
    results.ususp       =   (data[7] === '-' ? null : data[7]);
    results.rsv         =   (data[8] === '-' ? null : data[8]);
    results.dispatch    =   (data[9] === '-' ? null : data[9]);
    // Get the load if node is available
    if (results.status !== 'Unavailable'){
        //5th line is Total and 6th is Reserved load
        //9th and 10th are threshold
        var lines = {
            "totalLoad" : 4,
            "reservedLoad" : 5,
            "loadSched" : 8,
            "loadStop" : 9
        };
        var load;
        for (var j in lines){
            data = output[lines[j]].trim().split(/\s+/g);
            load = {};
            load.r15s   =   (data[1] === '-' ? null : data[1]);
            load.r1m    =   (data[2] === '-' ? null : data[2]);
            load.r15m   =   (data[3] === '-' ? null : data[3]);
            load.ut     =   (data[4] === '-' ? null : data[4]);
            load.pg     =   (data[5] === '-' ? null : data[5]);
            load.io     =   (data[6] === '-' ? null : data[6]);
            load.ls     =   (data[7] === '-' ? null : data[7]);
            load.it     =   (data[8] === '-' ? null : data[8]);
            load.tmp    =   (data[9] === '-' ? null : data[9]);
            load.swp    =   (data[10] === '-' ? null : data[10]);
            load.mem    =   (data[11] === '-' ? null : data[11]);
            results[j] = load;
        }
    }
    
    return results;
}

// Return the list of nodes
function lavanodes_js(lava_config, nodeName, callback){
    // JobId is optionnal so we test on the number of args
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    lava_config = args.shift();

    // last argument is the callback function
    callback = args.pop();
    
    var remote_cmd = lava_config.binaries_dir;
    
    // Info on a specific node
    if (args.length == 1){
        nodeName = args.pop();
        remote_cmd += cmdDict.node + nodeName;
    }else{
        remote_cmd += cmdDict.nodes;
    }
    
    var output = spawnProcess(remote_cmd,"shell",null,lava_config);
    
    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr));
    }
    
    var nodes = [];
    
    //Detect empty values
    //output = output.stdout.replace(/=,/g,"=null,");
    //Separate each node
    output = output.stdout.split(/HOST\s+/g);
    
    //Loop on each node
    for (var j = 0; j < output.length; j++) {
        if (output[j].length>1){
            //Split at lign breaks
            output[j]  = output[j].trim().split(/\n+/);
            nodes.push(jsonifyBhosts(output[j]));
        }
    }
    
    return callback(null, nodes);
}

module.exports = {
    lavanodes_js      : lavanodes_js,
};
