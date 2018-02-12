package stork.module.googleDrive;

import stork.feather.*;
import stork.module.*;

public class GoogleDriveModule extends Module<GoogleDriveResource> {
  {
    name("Stork Google Drive Module");
    protocols("googledrive");
    description("Experimental Google Drive module.");
  }

  public GoogleDriveResource select(URI uri, Credential credential) {
    URI endpoint = uri.endpointURI(), resource = uri.resourceURI();
    return new GoogleDriveSession(endpoint, credential).select(resource.path());
  }
}
