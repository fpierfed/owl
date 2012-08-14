#!/usr/bin/env python
"""
Create an empty blackboard table in whatever database is defined in owlrc.

Usage:
    shell> blackboard-init.py
"""
import elixir

from owl.config import DATABASE_CONNECTION_STR
from owl.blackboard import *



# Init the blackboard database.
elixir.metadata.bind = DATABASE_CONNECTION_STR
elixir.metadata.bind.echo = False
elixir.setup_all()
elixir.create_all()
