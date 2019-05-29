# coding: utf-8

from sqlalchemy import Column, Integer, String, ForeignKey, MetaData, Table, \
    create_engine
from sqlalchemy.orm import relationship, backref, sessionmaker, mapper

from sqlalchemy.ext.associationproxy import association_proxy


# --[ Schema and mapped classes ]------------------------------------------

metadata = MetaData()

filesystem_table = Table(
    "filesystem", metadata,
    Column("fsid", Integer, primary_key=True),
    Column("device", String, nullable=False),
    Column("backend", String, nullable=False),
)

entry_table = Table(
    "entry", metadata,
    Column("id", Integer, primary_key=True),
    Column("fsid", Integer, ForeignKey("filesystem.fsid"), nullable=False),
    Column("entry_type", String),
    Column("name", String),
)

file_table = Table(
    "file", metadata,
    Column("id", ForeignKey("entry.id"), primary_key=True),
    Column("content", String),
)

directory_entry_table = Table(
    "directory_entry", metadata,
    Column("dir_id", Integer, ForeignKey("entry.id"), primary_key=True),
    Column("entry_id", Integer, ForeignKey("entry.id"), primary_key=True),
)


class FileSystem(object):
    def __init__(self, device, backend):
        self.device = device
        self.backend = backend


class EntryCommon(object):
    def __init__(self, name, filesystem=None):
        self.name = name
        self.filesystem = filesystem

    @property
    def filesystem(self):
        return self._filesystem

    @filesystem.setter
    def filesystem(self, value):
        self._filesystem = filesystem

    def __repr__(self):
        return "<{0}: {1} ({2})>".format(self.__class__.__name__, self.name, self.id)


class File(EntryCommon):
    def __init__(self, name, content):
        super(File, self).__init__(name=name)
        self.content = content


class Directory(EntryCommon):
    entries = association_proxy("directory_entries", "entry")

    def __init__(self, name, entries=(), **kwargs):
        super(Directory, self).__init__(name=name, **kwargs)
        self.entries = entries

    @EntryCommon.filesystem.setter
    def filesystem(self, value):
        for entry in self.entries:
            entry.filesystem = value
        super(Directory, type(self)).filesystem.__set__(self, value)


class DirectoryEntry(object):

    def __init__(self, entry):
        super(DirectoryEntry, self).__init__()
        self.entry = entry

    def __repr__(self):
        return "<DirectoryEntry @ {0:x}>".format(id(self))


mapper(FileSystem, filesystem_table)
mapper(
    EntryCommon, entry_table,
    properties={"_filesystem": relationship(FileSystem)},
    polymorphic_on=entry_table.c.entry_type,
)
mapper(File, file_table, inherits=EntryCommon, polymorphic_identity="file")
mapper(Directory, local_table=None, inherits=EntryCommon, polymorphic_identity="directory")
mapper(DirectoryEntry, directory_entry_table, properties={
    "directory": relationship(
        Directory,
        primaryjoin=directory_entry_table.c.dir_id == entry_table.c.id,
        backref=backref("directory_entries", cascade='all,delete-orphan'),
    ),
    "entry": relationship(EntryCommon,
                          foreign_keys=[directory_entry_table.c.entry_id]),
})


# --[ Preparation ]------------------------------------------

engine = create_engine("sqlite:///", echo=False)
metadata.create_all(engine)

session = sessionmaker(engine)()

filesystem = FileSystem("/dev/sda", "btrfs")
session.add(filesystem)
session.commit()

documents = Directory(name="Documents")
videos = Directory(name="Videos")
documents.entries = [videos, Directory(name="Pictures")]
videos.entries = [File(name="dancing.mp4", content="Cha Cha, Slow Fox and more")]
documents.filesystem = filesystem
session.add(documents)
session.commit()

# Works fine, but accesses private relationship
print("Content of filesystem:")
print(session.query(EntryCommon).filter_by(_filesystem=filesystem).all())
