# ULog test fixtures

The two logs in this folder are third-party files. They are used only by the tests of the
ULog bridge: the `mosaicolabs` package does not import them, and they are not included in
its wheel or source distribution.

## Origin

Both files are copied **unchanged** from the `test/` folder of
[PX4/pyulog](https://github.com/PX4/pyulog) at release
[v1.2.4](https://github.com/PX4/pyulog/tree/v1.2.4/test), the library the bridge reads ULog
files with. They can be checked against upstream with the checksums below.

| File | Size (bytes) | SHA-256 | Added upstream |
|---|---|---|---|
| `sample_appended_multiple.ulg` | 486737 | `daf30f3224303e39d5c97701e048e84ba04480797e369502331f45ab2e99a2b7` | [dd44f43](https://github.com/PX4/pyulog/commit/dd44f431deb1f9b2075b7d513d41073b459b1109), 2017-07-05 |
| `sample_log_small.ulg` | 921631 | `68d1020f688109f027dba08d2950247cc0ae34aaefdcf37e4a7d60838f3e1aa3` | [0197eba](https://github.com/PX4/pyulog/commit/0197ebaad28a1d73c366acf0064311e3d289ec40), 2022-07-22 |

What they contain, exactly as published upstream:

- `sample_appended_multiple.ulg`: a 10-second log of a PX4FMU_V4PRO board, with data
  appended after the end of the log. It has no geographic position data.
- `sample_log_small.ulg`: a flight of about 20 minutes of a CubePilot Cube Orange. Like any
  PX4 flight log, it includes the GNSS position of the vehicle.

Both logs carry the identifier of the board that recorded them (`sys_uuid`).

## License

pyulog is distributed under the BSD 3-Clause license (SPDX `BSD-3-Clause`), which permits
redistribution with or without modification, provided that the copyright notice, the list
of conditions and the disclaimer are retained. They are reproduced below in full, from the
upstream [`LICENSE.md`](https://github.com/PX4/pyulog/blob/v1.2.4/LICENSE.md):

```
Copyright (c) 2016, PX4 Pro Drone Autopilot
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of GpsDrivers nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```
