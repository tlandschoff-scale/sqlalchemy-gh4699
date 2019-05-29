import contextlib

from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, backref, sessionmaker

from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.orderinglist import ordering_list


# --[ Schema and mapped classes ]------------------------------------------

Base = declarative_base()


class EntryCommon(Base):
    __tablename__ = "entry"
    id = Column(Integer, primary_key=True)
    entry_type = Column(String)
    name = Column(String)
    __mapper_args__ = {'polymorphic_on': entry_type}

    def __repr__(self):
        return "<{0}: {1} ({2})>".format(self.__class__.__name__, self.name, self.id)


class File(EntryCommon):
    __tablename__ = "file"
    id = Column(Integer, ForeignKey(EntryCommon.id), primary_key=True)
    content = Column(String)
    __mapper_args__ = {'polymorphic_identity': 'file'}


class Directory(EntryCommon):
    __tablename__ = "directory"
    id = Column(Integer, ForeignKey(EntryCommon.id), primary_key=True)
    entries = association_proxy("directory_entries", "entry")
    __mapper_args__ = {'polymorphic_identity': 'directory'}


class DirectoryEntry(Base):
    __tablename__ = "directory_entry"
    dir_id = Column(Integer, ForeignKey("directory.id"), primary_key=True)
    entry_id = Column(Integer, ForeignKey("entry.id"), primary_key=True)

    directory = relationship(Directory,
        primaryjoin=Directory.id == dir_id,
        backref=backref("directory_entries",
                        cascade='all,delete-orphan')
    )

    entry = relationship(EntryCommon)

    def __init__(self, entry, **kwargs):
        super(DirectoryEntry, self).__init__(entry=entry, **kwargs)

    def __repr__(self):
        return "<DirectoryEntry @ {0:x}>".format(id(self))


# --[ Preparation ]------------------------------------------

engine = create_engine("sqlite:///", echo=False)
Base.metadata.create_all(engine)

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
