Set up trusted copy between postgres accounts
---------------------------------------------

If you need to use `rsync` to clone standby servers, the `postgres` account
on your primary and standby servers must be each able to access the other
using SSH without a password.

First generate an ssh key, using an empty passphrase, and copy the resulting
keys and a matching authorization file to a privileged user account on the other
system:

    [postgres@node1]$ ssh-keygen -t rsa
    Generating public/private rsa key pair.
    Enter file in which to save the key (/var/lib/pgsql/.ssh/id_rsa):
    Enter passphrase (empty for no passphrase):
    Enter same passphrase again:
    Your identification has been saved in /var/lib/pgsql/.ssh/id_rsa.
    Your public key has been saved in /var/lib/pgsql/.ssh/id_rsa.pub.
    The key fingerprint is:
    aa:bb:cc:dd:ee:ff:aa:11:22:33:44:55:66:77:88:99 postgres@db1.domain.com
    [postgres@node1]$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    [postgres@node1]$ chmod go-rwx ~/.ssh/*
    [postgres@node1]$ cd ~/.ssh
    [postgres@node1]$ scp id_rsa.pub id_rsa authorized_keys user@node2:

Login as a user on the other system, and install the files into the `postgres`
user's account:

    [user@node2 ~]$ sudo chown postgres.postgres authorized_keys id_rsa.pub id_rsa
    [user@node2 ~]$ sudo mkdir -p ~postgres/.ssh
    [user@node2 ~]$ sudo chown postgres.postgres ~postgres/.ssh
    [user@node2 ~]$ sudo mv authorized_keys id_rsa.pub id_rsa ~postgres/.ssh
    [user@node2 ~]$ sudo chmod -R go-rwx ~postgres/.ssh

Now test that ssh in both directions works.  You may have to accept some new
known hosts in the process.
