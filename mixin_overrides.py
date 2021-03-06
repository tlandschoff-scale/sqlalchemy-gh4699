# coding: utf-8
import abc
import json
import pickle
import sys

from sqlalchemy import Column, Integer, String, ForeignKey, \
    create_engine, Boolean
from sqlalchemy.ext.declarative import declarative_base, synonym_for, DeclarativeMeta, declared_attr
from sqlalchemy.orm import relationship, backref, sessionmaker

from sqlalchemy.ext.associationproxy import association_proxy


# --[ Schema and mapped classes ]------------------------------------------

class ABCDeclarativeMeta(DeclarativeMeta, abc.ABCMeta):
    pass


Base = declarative_base(metaclass=ABCDeclarativeMeta)


class EntryInterface(metaclass=abc.ABCMeta):
    """
    Describes the interface that mixin classes for :meth:`EntryCommon` may
    rely on. For example, ResourcesBearer relies on this interface to access
    `EntryCommon` features which it can not inherit from due to SQLAlchemy
    supporting single inheritance from mapped classes only.
    """
    @property
    @abc.abstractmethod
    def filesystem(self):
        pass

    @filesystem.setter
    @abc.abstractmethod
    def filesystem(self, value):
        pass


class FileSystem(Base):
    __tablename__ = "filesystem"

    fsid = Column(Integer, primary_key=True)
    device = Column(String, nullable=False)
    backend = Column(String, nullable=False)

    def __init__(self, device, backend):
        self.device = device
        self.backend = backend


class EntryCommon(Base, EntryInterface):
    __tablename__ = "entry"

    id = Column(Integer, primary_key=True)
    fsid = Column(Integer, ForeignKey("filesystem.fsid"), nullable=False)
    entry_type = Column("entry_type", String)
    name = Column("name", String)

    __mapper_args__ = {"polymorphic_on": entry_type}
    _filesystem = relationship(FileSystem)

    def __init__(self, name, filesystem=None):
        self.name = name
        self.filesystem = filesystem

    @property
    def filesystem(self):
        return self._filesystem

    @synonym_for("_filesystem")
    @filesystem.setter
    def filesystem(self, value):
        self._filesystem = filesystem

    def __repr__(self):
        return "<{0}: {1} ({2})>".format(self.__class__.__name__, self.name, self.id)


class Resource(Base):
    __tablename__ = "resources"

    id = Column(ForeignKey("entry.id"), primary_key=True)
    name = Column(String, primary_key=True)
    fsid = Column(Integer, ForeignKey("filesystem.fsid"), nullable=False)
    value = Column(String)

    owner = relationship(EntryCommon)
    filesystem = relationship(FileSystem)

    def __init__(self, name, value):
        self.name = name
        self.value = value


class ResourcesBearer(EntryInterface):
    """Mixin class to provide resource forks to filesystem entries."""

    @declared_attr
    def resource_enc(self):
        return Column("resource_enc", String)

    @declared_attr
    def _resources(self):
        return relationship(Resource)

    def __init__(self, resource_enc="json", **kwargs):
        super(ResourcesBearer, self).__init__(**kwargs)
        self.resource_enc = resource_enc

    @property
    def filesystem(self):
        return super(ResourcesBearer, self).filesystem

    @filesystem.setter
    def filesystem(self, value):
        super(ResourcesBearer, type(self)).filesystem.__set__(self, value)
        for res in self._resources:
            res.filesystem = value

    def add_resource(self, name, value):
        encoder = self._lookup_encoder()
        self._resources.append(Resource(name=name, value=encoder.dumps(value)))

    def _lookup_encoder(self):
        return {
            "json": json,
            "pickle": pickle
        }[self.resource_enc]


class File(EntryCommon):
    __tablename__ = "file"
    id = Column(ForeignKey("entry.id"), primary_key=True)
    content = Column(String)

    __mapper_args__ = {"polymorphic_identity": "file"}

    def __init__(self, name, content):
        super(File, self).__init__(name=name)
        self.content = content


class Directory(ResourcesBearer, EntryCommon):
    __tablename__ = "directory"

    id = Column(ForeignKey("entry.id"), primary_key=True)
    __mapper_args__ = {"polymorphic_identity": "directory"}

    entries = association_proxy("directory_entries", "entry")

    def __init__(self, name, entries=(), **kwargs):
        super(Directory, self).__init__(name=name, **kwargs)
        self.entries = entries

    @synonym_for("_filesystem")
    @EntryCommon.filesystem.setter
    def filesystem(self, value):
        super(Directory, type(self)).filesystem.__set__(self, value)
        for entry in self.entries:
            entry.filesystem = value


class DirectoryEntry(Base):
    __tablename__ = "directory_entry"

    dir_id = Column("dir_id", Integer, ForeignKey("directory.id"), primary_key=True)
    entry_id = Column("entry_id", Integer, ForeignKey("entry.id"), primary_key=True)

    directory = relationship(
        Directory,
        primaryjoin=dir_id == Directory.id,
        backref=backref("directory_entries", cascade='all,delete-orphan'),
    )

    entry = relationship(EntryCommon, foreign_keys=[entry_id])

    def __init__(self, entry):
        super(DirectoryEntry, self).__init__()
        self.entry = entry

    def __repr__(self):
        return "<DirectoryEntry @ {0:x}>".format(id(self))


class Executable(ResourcesBearer, File):
    __tablename__ = "executable"

    id = Column(ForeignKey("file.id"), primary_key=True)
    windowed = Column(Boolean)
    __mapper_args__ = {'polymorphic_identity': "executable"}

    def __init__(self, name, content=None, windowed=False, **kwargs):
        super(Executable, self).__init__(name=name, content=content, **kwargs)
        self.windowed = windowed


# --[ Preparation ]------------------------------------------

database_url = sys.argv[1] if len(sys.argv) > 1 else "sqlite:///"
engine = create_engine(database_url, echo=False)
Base.metadata.create_all(engine)

session = sessionmaker(engine)()

filesystem = FileSystem("/dev/sda", "btrfs")
session.add(filesystem)
session.commit()

documents = Directory(name="Documents")
videos = Directory(name="Videos")
videos.add_resource("Icon", {"width": 32, "height": 32, "data": "film reel"})
documents.entries = [videos, Directory(name="Pictures")]
videos.entries = [File(name="dancing.mp4", content="Cha Cha, Slow Fox and more")]
documents.filesystem = filesystem
session.add(documents)
programs = Directory(name="Programs")
cmd_exe = Executable(name="cmd.exe", resource_enc="pickle")
cmd_exe.add_resource("Icon", {"width": 32, "height": 32, "data": "binary icon"})
programs.entries = [cmd_exe]
programs.filesystem = filesystem
session.add(programs)
session.commit()

print("Content of filesystem:")
print(session.query(EntryCommon).filter_by(filesystem=filesystem).all())

print("Directories on filesystem:")
print(session.query(Directory).filter_by(filesystem=filesystem).all())

print(Directory, Directory.__mro__)
print(Executable, Executable.__mro__)
