#!/bin/bash

# From http://docs.cloudlab.us/advanced-topics.html#%28part._geni-get-key%29
setup_key() {
  # Retrieve the server-generated RSA private key.
  geni-get key > $HOME/.ssh/id_rsa
  chmod 600 $HOME/.ssh/id_rsa

  # Derive the corresponding public key portion.
  ssh-keygen -y -f $HOME/.ssh/id_rsa > $HOME/.ssh/id_rsa.pub

  # If you want to permit login authenticated by the auto-generated key,
  # then append the public half to the authorized_keys file:
  grep -q -f $HOME/.ssh/id_rsa.pub $HOME/.ssh/authorized_keys || cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
}

setup_key
sudo -i
setup_key
