# coding: utf-8

from sqlalchemy import Column, Integer, String, ForeignKey, MetaData, Table, \
    create_engine
from sqlalchemy.orm import relationship, backref, sessionmaker, mapper

from sqlalchemy.ext.associationproxy import association_proxy


# --[ Schema and mapped classes ]------------------------------------------

metadata = MetaData()

entry_table = Table(
    "entry", metadata,
    Column("id", Integer, primary_key=True),
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


class EntryCommon(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<{0}: {1} ({2})>".format(self.__class__.__name__, self.name, self.id)


class File(EntryCommon):
    def __init__(self, name, content):
        super(File, self).__init__(name=name)
        self.content = content


class Directory(EntryCommon):
    entries = association_proxy("directory_entries", "entry")

    def __init__(self, name, entries=()):
        super(Directory, self).__init__(name=name)
        self.entries = entries


class DirectoryEntry(object):

    def __init__(self, entry):
        super(DirectoryEntry, self).__init__()
        self.entry = entry

    def __repr__(self):
        return "<DirectoryEntry @ {0:x}>".format(id(self))


mapper(EntryCommon, entry_table, polymorphic_on=entry_table.c.entry_type)
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

engine = create_engine("sqlite:///", echo=True)
metadata.create_all(engine)

session = sessionmaker(engine)()


documents = Directory(name="Documents")
videos = Directory(name="Videos")
documents.entries = [videos, Directory(name="Pictures")]
videos.entries = [File(name="dancing.mp4", content="Cha Cha, Slow Fox and more")]
session.add(documents)
session.commit()


# --[ Triggering the problem ]------------------------------------

print("Initial session content: {0}".format(list(session)))
with session.begin(subtransactions=True):
    new_videos = Directory(name="New Videos")
    new_videos.entries = videos.entries
    documents.entries[0] = new_videos
    session.flush()  # this is required to trigger the problem

    video = File(name="falcon_landing.mp4", content="Space Ship Landing")
    new_videos.entries[0] = video
    # new_videos.entries.append(video)  # instead of replacing works. wtf?

print("Pre rollback session content: {0}".format(list(session)))
session.rollback()

print("After rollback session content: {0}".format(list(session)))
# del new_videos  # this magically makes it work!?

more_videos = Directory(name="More Videos")
more_videos.entries = videos.entries
documents.entries[0] = more_videos

session.commit()
