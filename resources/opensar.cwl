cwlVersion: v1.2
$namespaces:
  s: https://schema.org/
s:softwareVersion: 1.0.0
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:
  - class: Workflow
    label: OST Notebook 1
    doc: Preprocessing an S1 image with OST
    id: opensartoolkit
    requirements: []
    inputs:
      input:
        type: Directory
        label: Input S1 GRD
        loadListing: no_listing
      resolution:
        type: int
        label: Resolution
        doc: Resolution in metres
      ard-type:
        type:
          type: enum
          symbols:
            - OST_GTC
            - OST-RTC
            - CEOS
            - Earth-Engine
        label: ARD type
        doc: Type of analysis-ready data to produce
      with-speckle-filter:
        type: boolean
        label: Speckle filter
        doc: Whether to apply a speckle filter
      resampling-method:
        type:
          type: enum
          symbols:
            - BILINEAR_INTERPOLATION
            - BICUBIC_INTERPOLATION
        label: Resampling method
        doc: Resampling method to use
      dry-run:
        type: boolean
        label: Dry run
        doc: Skip processing and write a placeholder output file instead

    outputs:
      - id: stac_catalog
        outputSource:
          - run_script/ost_ard
        type: Directory

    steps:
      run_script:
        run: "#ost_script_1"
        in:
          input: input
          resolution: resolution
          ard-type: ard-type
          with-speckle-filter: with-speckle-filter
          resampling-method: resampling-method
          dry-run: dry-run
        out:
          - ost_ard

  - class: CommandLineTool
    id: ost_script_1
    requirements:
      DockerRequirement:
        dockerPull: quay.io/bcdev/opensartoolkit:version8
      NetworkAccess:
        networkAccess: true

    baseCommand:
      - python3
      - /usr/local/lib/python3.8/dist-packages/ost/app/preprocessing.py
    arguments:
      - --wipe-cwd
    inputs:
      input:
        type: Directory
        inputBinding:
          position: 1
      resolution:
        type: int
        inputBinding:
          prefix: --resolution
      ard-type:
        type:
          type: enum
          symbols:
            - OST_GTC
            - OST-RTC
            - CEOS
            - Earth-Engine
        inputBinding:
          prefix: --ard-type
      with-speckle-filter:
        type: boolean
        inputBinding:
          prefix: --with-speckle-filter
      resampling-method:
        type:
          type: enum
          symbols:
            - BILINEAR_INTERPOLATION
            - BICUBIC_INTERPOLATION
        inputBinding:
          prefix: --resampling-method
      cdse-user:
        type: string?
        inputBinding:
          prefix: --cdse-user
      cdse-password:
        type: string?
        inputBinding:
          prefix: --cdse-password
      dry-run:
        type: boolean
        inputBinding:
          prefix: --dry-run

    outputs:
      ost_ard:
        outputBinding:
          glob: .
        type: Directory
