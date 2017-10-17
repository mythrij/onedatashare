package stork.module.googleDrive;

import stork.feather.Credential;
import stork.feather.URI;
import stork.module.Module;
import stork.module.dropbox.DbxResource;
import stork.module.dropbox.DbxSession;

public class GoogleDriveModule extends Module<DbxResource> {
  {
    name("Stork Google Drive Module");
    protocols("gdrive");
    description("Experimental Google Drive module.");
  }

  public DbxResource select(URI uri, Credential credential) {
    URI endpoint = uri.endpointURI(), resource = uri.resourceURI();
    return new DbxSession(endpoint, credential).select(resource.path());
  }
}
