/**

Session extension
*****************

The `session extension <https://www.sqlite.org/sessionintro.html>`
allows recording changes to a database, and later replaying them on
another database, or undoing them.  This allows offline syncing, as
well as collaboration.  It is also useful for debugging, development,
and testing.

Notable features include:

* You can choose which tables have changes recorded (or all), and
  pause / resume recording at any time

* The recorded change set includes what values were before a change so
  conflicts can be detected.  Optionally you can use patch sets (a
  subset of change sets) which do not have the before values,
  consuming less space but have less ability to detect conflicts.

* When applying changes you can supply a conflict handler to choose
  what happens on each conflict, including aborting, skipping,
  applying anyway, applying your own change, and can record the
  conflicting operation to another change set for later.

* You can iterate over a change set to see what it contains

* Using the change set builder, you can accumulate multiple change
  sets, and add changes from an iterator or conflict handler.

 */