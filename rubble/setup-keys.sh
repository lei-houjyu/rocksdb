#!/bin/bash

# From http://docs.cloudlab.us/advanced-topics.html#%28part._geni-get-key%29
setup_key() {
  DIR=$1
  
  # Retrieve the server-generated RSA private key.
  geni-get key > $DIR/.ssh/id_rsa
  chmod 600 $DIR/.ssh/id_rsa

  # Derive the corresponding public key portion.
  ssh-keygen -y -f $DIR/.ssh/id_rsa > $DIR/.ssh/id_rsa.pub

  # If you want to permit login authenticated by the auto-generated key,
  # then append the public half to the authorized_keys file:
  grep -q -f $DIR/.ssh/id_rsa.pub $DIR/.ssh/authorized_keys || cat $DIR/.ssh/id_rsa.pub >> $DIR/.ssh/authorized_keys
}

setup_key $HOME
setup_key /root
