# iSamples Export Service

The iSamples export service export iSamples records that match [solr queries](https://solr.apache.org/guide/solr/latest/query-guide/query-syntax-and-parsers.html).  Only users that have been authenticated using [orcid](https://orcid.org) are allowed to access the export services.  Additionally, users have to be granted access using the iSamples admin API in order to export results.

## Logging In
All of the export services are protected by authentication and require a JWT.  The login URL is at [iSamples Central](https://central.isample.xyz/isamples_central/manage/login?raw_jwt=true).  Make sure your orcid is allowed by talking to your iSamples in a Box administrator.  

Once you've obtained your JWT, you include it in curl requests like so:

`curl -H "Authorization: Bearer <JWT>"` 

Note that for brevity's sake, none of the examples in this document include a JWT, but if it is omitted in the curl request, it most certainly shouldn't work.

## Creating an export job
The first step in the export process is to create an export job.  An example workflow follows:

```
isamples_inabox % curl  "https://central.isample.xyz/isamples_central/export/create?q=source:SMITHSONIAN&export_format=jsonl"
{"status":"created","uuid":"0d64cf54-24a3-410f-9da3-ef961e78821e"}
```

The response contains the uuid you will use to refer to the job while it is running and after it is completed.

## Checking the export job status
While the solr export is working, you may query the status of the job as follows:

```
isamples_inabox % curl  "https://central.isample.xyz/isamples_central/export/status?uuid=0d64cf54-24a3-410f-9da3-ef961e78821e"
{"status":"started","tstarted":"2024-03-15 06:21:48.227189"}
``` 
In this instance, we can see the export has started but isn't yet complete.  If we check back later, we can see the job is done:

```
isamples_inabox % curl  "https://central.isample.xyz/isamples_central/export/status?uuid=0d64cf54-24a3-410f-9da3-ef961e78821e"
{"status":"completed","tcompleted":"2024-03-15 06:24:37.488125"}
```

## Downloading the result
Once we see that the job has a "completed" status, we can download the result like so:

```
isamples_inabox % curl "https://central.isample.xyz/isamples_central/export/download?uuid=0d64cf54-24a3-410f-9da3-ef961e78821e"
{"sample_identifier": "IGSN:BSU0005H1", "label": "BJJ-4487", "description": null, "source_collection": "SESAR", "has_specimen_category": ["Other solid object"], "has_material_category": ["Rock"], "has_context_category": ["Earth interior"], "informal_classification": ["Not Provided"], "keywords": ["Individual Sample"], "produced_by": {"responsibility": ["Brad Johnson,,Collector", "Mark Schmitz,,Sample Owner"], "description": "cruiseFieldPrgrm:STATEMAP 2015, Arizona Geological Survey. Rhyolitic metatuff, massive to foliated, containing 10-15% white feldspar grains 1-2 mm (angular fragments) and trace 1-mm quartz grains in aphanitic white to pale green matrix (groundmass); also contains light-green porphyroblasts, typically 1-3 mm but up to 1 cm locally, subspherical to ellipsoidal (with long axes parallel to foliation), that weather in relief and form 10% to locally 50% of the rock.  The porphyroblasts contain feldspar phenocrysts as in the matrix, and probably grew over them; also there are a few 1-cm-thick veins of same color (and probably composition)", "result_time": "2019-09-10T03:41:45Z", "has_feature_of_interest": "northern Santa Rita Mtns.", "sampling_site": {"place_name": ["Arizona", "Corona de Tucson", "Pima", "northern Santa Rita Mtns."], "sample_location": {}, "elevation": "", "latitude": 31.8854, "longitude": -110.7733}}, "registrant": {"name": ["Mark Schmitz"]}, "sampling_purpose": null, "curation": {"label": "", "description": "", "access_constraints": "", "curation_location": ""}, "related_resource": null, "authorized_by": null, "complies_with": null}
```
and we can see the contents of the file echoed to the command line by curl.

### Empty Results

In case the query didn't return any results, the download response is expected to return an empty result with an http status code 204 (No Content).