
# From
# Real-Time Collision Detection
# Christer Ericson
# Elsevier, San Fransisco
# (C) 2005
#

class Point( object):
    __slots__ = ('x', 'y', 'z')
    def __init__( self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def __repr__( self):
        return "Point( %s, %s, %s)" % ( self.x.__repr__(), self.y.__repr__(), self.z.__repr__())

    def __str__( self):
        return "Point( %f, %f, %f)" % ( self.x, self.y, self.z)

    def __sub__( self, b):
        return Vector( self.x - b.x, self.y - b.y, self.z - b.z)


class Vector( object):
    __slots__ = ('x', 'y', 'z')
    def __init__( self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def __repr__( self):
        return "Vector( %s, %s, %s)" % ( self.x.__repr__(), self.y.__repr__(), self.z.__repr__())

    def __str__( self):
        return "Vector( %f, %f, %f)" % ( self.x, self.y, self.z)

    def dot( self, a):
        return self.x * a.x + self.y * a.y + self.z * a.z

    def __sub__( self, b):
        return Vector( self.x - b.x, self.y - b.y, self.z - b.z)


class Rotation( object):
    a = [[0,0,0],[0,0,0],[0,0,0]]
    b = [[0,0,0],[0,0,0],[0,0,0]]
    c = [[0,0,0],[0,0,0],[0,0,0]]

    def __init__( self, dx, dy, dz):
        rx = dx * math.pi/180.
        ry = dy * math.pi/180.
        rz = dz * math.pi/180.

        self.a[0][0] = 1
        self.a[1][1] = math.cos( rz)
        self.a[2][2] = self.a[1][1]
        self.a[2][1] = math.size( rz)
        self.a[1][2] = -self.a[2][1]

        self.b[0][0] = math.cos( ry)
        self.b[2][2] = self.b[0][0]
        self.b[1][1] = 1
        self.b[0][2] = math.sin( ry)
        self.b[2][0] = -self.b[0][2]

        

class Sphere( object):
    __slots__ = ('c', 'r')
    def __init__( self, c, r):
        self.c = c
        self.r = r

    def __repr__( self):
        return "Sphere( %s, %s)" % (self.c.__repr__(), self.r.__repr__())

    def __str__( self):
        return "Sphere( %s, %f)" % (self.c.__str__(), self.r)

class Capsule( object):
    __slots__ = ('a', 'b', 'r')
    def __init__( self, a, b, r):
        self.a = a
        self.b = b
        self.r = r

    def __repr__( self):
        return "Capsule( %s, %s, %s)" % ( self.a.__repr__(), self.b.__repr__(), self.r.__repr__())

    def __str__( self):
        return "Capsule( %s, %s, %f)" % ( self.a.__str__(), self.b.__str__(), self.r)

    def SqDistPointSegment( self, c):
        #
        # Ibid p. 130
        #
        # returns squared distance between point c and capsule defining line segment
        ab = self.b - self.a
        ac = c - self.a
        bc = c - self.b

        e = ac.dot(ab)

        # Handle cases where c projects outside ab
        if e <= 0.0:
            return ac.dot(ac)

        f = ab.dot(ab)
        if e >= f:
            return bc.dot(bc)

        # handle cases where c projects onto ab
        return ac.dot(ac) - e*e/f

    def TestSphereCapsule( self, s):
        #
        # Ibid p. 114
        #
        # Compute squared distance between sphere center and capsule line segment
        dist2 = self.SqDistPointSegment( s.c)

        # sum of radii
        radius = s.r + self.r

        # If squared distance is smaller than squared sum of radii, they colllide
        return dist2 <= radius * radius



if __name__ == "__main__":
    p1 = Point( 0, 0, 0)
    p2 = Point( 10, 10, 10)
    v1 = Vector( 1, 0, 0)
    v2 = Vector( 0, 1, 0)
    v3 = Vector( 0, 0, 1)
    s  = Sphere( Point(-100, 0, 0), .6)

    c = Capsule( p1, p2, 2)

    print "p1:", p1, p1.__repr__()
    print "p2:", p2, p2.__repr__()
    print "v1:", v1, v1.__repr__()
    print "v2:", v2, v2.__repr__()
    print "v3:", v3, v3.__repr__()

    print "s:", s, s.__repr__()
    print "c:", c, c.__repr__()

    print "v2 - v1", v2-v1
    print "v2.dot(v1)", v2.dot(v1)

    print "Collides? ", c.TestSphereCapsule( s)


    
