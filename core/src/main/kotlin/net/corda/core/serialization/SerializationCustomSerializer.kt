package net.corda.core.serialization

import java.lang.reflect.Type

/**
 * Allows CorDapps to provide custom serializers for third party libraries where those libraries cannot
 * be recompiled with the -parmater flag rendering their classes natively serializable by Corda. In this case
 * a proxy serializer can be written that extends this type whose purpose is to move between those an
 * unserializable types and an intermediate representation
 *
 * NOTE: The proxy object must be specified as a seperate class. However, this can be defined within the
 * scope of the serializer. Also, that class must be annotated with the [CordaCustomSerializerProxy]
 * annotation
 *
 * For instances of this class to be discoverable they must be annotated with the [CordaCustomSerializer]
 * annotation.
 *
 * Failing to apply either annotation will result in the class not being loaded by Corda and thus serialization
 * failing
 *
 * @property type the type of the object that this class is proxying
 * @property ptype the type of the proxy object used as an intermediate representation of [type]
 */
interface SerializationCustomSerializer {
    /**
     * Should facilitate the conversion of the third party object into the serializable
     * local class specified by [ptype]
     */
    fun toProxy(obj: Any) : Any

    /**
     * Should facilitate the conversion of the proxy object into a new instance of the
     * unserializable type
     */
    fun fromProxy(proxy: Any) : Any

    /**
     * Should be set to the type of the object being proxied
     */
    val type: Type

    /**
     * Should be set to the proxy objects type
     */
    val ptype: Type
}