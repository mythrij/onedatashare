package stork.module.dropbox;

import java.util.*;

import com.dropbox.core.*;

import stork.feather.*;
import stork.feather.util.*;

public class DbxResource extends Resource<DbxSession, DbxResource> {
  DbxResource(DbxSession session, Path path) {
    super(session, path);
  }

  public synchronized Emitter<String> list() {
    final Emitter<String> emitter = new Emitter<String>();
    new ThreadBell() {
      public Object run() throws Exception {
        DbxEntry.WithChildren listing = session.client.getMetadataWithChildren(path.toString());
        for (DbxEntry child : listing.children)
          emitter.emit(child.name);
        emitter.ring();
        return null;
      }
    }.startOn(initialize());
    return emitter;
  }

  public synchronized Bell<Stat> stat() {
    return new ThreadBell<Stat>() {
      public Stat run() throws Exception {
        DbxEntry dbe = session.client.getMetadata(path.toString());
        System.out.println("Doing dbx stat: "+dbe);

        if (dbe == null)
          throw new RuntimeException("File does not exist");

        Stat st = entryToStat(dbe);

        if (st.dir) {
          DbxEntry.WithChildren dbd = session.client.getMetadataWithChildren(path.toString());
          List<Stat> sub = new LinkedList<Stat>();
          for (DbxEntry child : dbd.children)
            sub.add(entryToStat(child));
          st.setFiles(sub);
        }

        return st;
      }
    }.startOn(initialize());
  }

  private Stat entryToStat(DbxEntry dbe) {
    Stat stat = new Stat(dbe.name);

    if (stat.file = dbe.isFile()) {
      DbxEntry.File file = dbe.asFile();
      stat.size = file.numBytes;
      stat.time = file.lastModified.getTime()/1000;
    } else {
      stat.dir = true;
    }

    return stat;
  }
}
